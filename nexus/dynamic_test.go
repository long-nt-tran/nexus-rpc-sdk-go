package nexus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type dynamicEcho struct {
	UnimplementedOperation[*LazyValue, any]
	cancelled atomic.Int32
}

func (d *dynamicEcho) Start(ctx context.Context, input *LazyValue, _ StartOperationOptions) (HandlerStartOperationResult[any], error) {
	var value int
	if err := input.Consume(&value); err != nil {
		return nil, err
	}
	return &HandlerStartOperationResultAsync{OperationToken: fmt.Sprintf("%s:%d", ExtractHandlerInfo(ctx).Operation, value)}, nil
}
func (d *dynamicEcho) Cancel(_ context.Context, _ string, _ CancelOperationOptions) error {
	d.cancelled.Add(1)
	return nil
}

func TestDynamicOperationRouting(t *testing.T) {
	service := NewService("tools")
	dynamic := &dynamicEcho{}
	require.NoError(t, service.RegisterDynamicOperation(dynamic))
	require.Error(t, service.RegisterDynamicOperation(dynamic))
	registry := NewServiceRegistry()
	registry.MustRegister(service)
	handler, err := registry.NewHandler()
	require.NoError(t, err)
	ctx := WithHandlerContext(context.Background(), HandlerInfo{Service: "tools", Operation: "echo"})
	result, err := handler.StartOperation(ctx, "tools", "echo", lv(t, 5), StartOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, "echo:5", result.(*HandlerStartOperationResultAsync).OperationToken)
	static := NewSyncOperation("echo", func(context.Context, int, StartOperationOptions) (string, error) { return "static", nil })
	require.NoError(t, service.Register(static))
	result, err = handler.StartOperation(ctx, "tools", "echo", lv(t, 5), StartOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, "static", result.(*HandlerStartOperationResultSync[string]).Value)
	require.NoError(t, service.Delete("echo"))
	require.NoError(t, handler.CancelOperation(ctx, "tools", "echo", "token", CancelOperationOptions{}))
	require.EqualValues(t, 1, dynamic.cancelled.Load())
	_, err = handler.StartOperation(ctx, "missing", "echo", lv(t, 5), StartOperationOptions{})
	require.Error(t, err)
}

func TestRuntimeOperationMutation(t *testing.T) {
	service := NewService("tools")
	service.MustRegister(numberValidatorOperation)
	registry := NewServiceRegistry()
	registry.MustRegister(service)
	handler, err := registry.NewHandler()
	require.NoError(t, err)
	entered, proceed := make(chan struct{}), make(chan struct{})
	old := NewSyncOperation("echo", func(context.Context, string, StartOperationOptions) (string, error) {
		close(entered)
		<-proceed
		return "old", nil
	})
	require.NoError(t, service.Register(old))
	ctx := WithHandlerContext(context.Background(), HandlerInfo{Service: "tools", Operation: "echo"})
	done := make(chan error, 1)
	go func() {
		result, err := handler.StartOperation(ctx, "tools", "echo", lv(t, "input"), StartOperationOptions{})
		if err == nil && result.(*HandlerStartOperationResultSync[string]).Value != "old" {
			err = fmt.Errorf("wrong handler captured")
		}
		done <- err
	}()
	<-entered
	replacement := NewSyncOperation("echo", func(context.Context, int, StartOperationOptions) (int, error) { return 42, nil })
	require.NoError(t, service.Update(replacement))
	result, err := handler.StartOperation(ctx, "tools", "echo", lv(t, 5), StartOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, 42, result.(*HandlerStartOperationResultSync[int]).Value)
	require.NoError(t, service.Delete("echo"))
	close(proceed)
	require.NoError(t, <-done)
	require.Error(t, service.Update(replacement))
	require.Error(t, service.Delete("echo"))
	_, err = handler.StartOperation(ctx, "tools", "echo", lv(t, 5), StartOperationOptions{})
	require.Error(t, err)
}

type cancelFallback struct {
	UnimplementedOperation[any, any]
	calls atomic.Int32
}

func (c *cancelFallback) Cancel(context.Context, string, CancelOperationOptions) error {
	c.calls.Add(1)
	return nil
}

func TestUnknownOperationCancelFallback(t *testing.T) {
	service := NewService("tools")
	service.MustRegister(numberValidatorOperation, asyncNumberValidatorOperationInstance)
	registry := NewServiceRegistry()
	registry.MustRegister(service)
	fallback := &cancelFallback{}
	registry.SetCancelFallback(fallback)
	handler, err := registry.NewHandler()
	require.NoError(t, err)
	ctx := WithHandlerContext(context.Background(), HandlerInfo{Service: "tools", Operation: "async-number-validator"})
	require.NoError(t, handler.CancelOperation(ctx, "tools", "async-number-validator", "token", CancelOperationOptions{}))
	require.Zero(t, fallback.calls.Load())
	require.NoError(t, service.Delete("async-number-validator"))
	require.NoError(t, handler.CancelOperation(ctx, "tools", "async-number-validator", "token", CancelOperationOptions{}))
	require.EqualValues(t, 1, fallback.calls.Load())
	require.Error(t, handler.CancelOperation(ctx, "missing", "async-number-validator", "token", CancelOperationOptions{}))
	require.EqualValues(t, 1, fallback.calls.Load())
}

func TestConcurrentOperationUpdates(t *testing.T) {
	service := NewService("tools")
	service.MustRegister(numberValidatorOperation)
	registry := NewServiceRegistry()
	registry.MustRegister(service)
	handler, err := registry.NewHandler()
	require.NoError(t, err)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				ctx := WithHandlerContext(context.Background(), HandlerInfo{Service: "tools", Operation: numberValidatorOperation.Name()})
				_, err := handler.StartOperation(ctx, "tools", numberValidatorOperation.Name(), lv(t, 7), StartOperationOptions{})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 100 {
		require.NoError(t, service.Update(numberValidatorOperation))
	}
	wg.Wait()
}
