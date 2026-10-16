package nexus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// NoValue is a marker type for an operations that do not accept any input or return a value (nil).
//
//	nexus.NewSyncOperation("my-empty-operation", func(context.Context, nexus.NoValue, options, nexus.StartOperationOptions) (nexus.NoValue, error) {
//		return nil, nil
//	)}
type NoValue *struct{}

// OperationReference provides a typed interface for invoking operations. Every [Operation] is also an
// [OperationReference]. Callers may create references using [NewOperationReference] when the implementation is not
// available.
type OperationReference[I, O any] interface {
	Name() string
	// InputType the generic input type I for this operation.
	InputType() reflect.Type
	// OutputType the generic out type O for this operation.
	OutputType() reflect.Type
	// A type inference helper for implementations of this interface.
	inferType(I, O)
}

type operationReference[I, O any] string

// NewOperationReference creates an [OperationReference] with the provided type parameters and name.
// It provides typed interface for invoking operations when the implementation is not available to the caller.
func NewOperationReference[I, O any](name string) OperationReference[I, O] {
	return operationReference[I, O](name)
}

func (r operationReference[I, O]) Name() string {
	return string(r)
}

func (operationReference[I, O]) InputType() reflect.Type {
	var zero [0]I
	return reflect.TypeOf(zero).Elem()
}

func (operationReference[I, O]) OutputType() reflect.Type {
	var zero [0]O
	return reflect.TypeOf(zero).Elem()
}

func (operationReference[I, O]) inferType(I, O) {} //nolint:unused

// A RegisterableOperation is accepted in [OperationRegistry.Register].
// Embed [UnimplementedOperation] to implement it.
type RegisterableOperation interface {
	// Name of the operation. Used for invocation and registration.
	Name() string
	mustEmbedUnimplementedOperation()
}

// Operation is a handler for a single operation.
//
// Operation implementations must embed the [UnimplementedOperation].
//
// See [OperationHandler] for more information.
type Operation[I, O any] interface {
	RegisterableOperation
	OperationReference[I, O]
	OperationHandler[I, O]
}

// OperationHandler is the interface for the core operation methods. OperationHandler implementations must embed
// [UnimplementedOperation].
//
// All Operation methods can return a [HandlerError] to fail requests with a custom [HandlerErrorType] and structured [Failure].
// Arbitrary errors from handler methods are turned into [HandlerErrorTypeInternal], when using the Nexus SDK's
// HTTP handler, their details are logged and hidden from the caller. Other handler implementations may expose internal
// error information to callers.
type OperationHandler[I, O any] interface {
	// Start handles requests for starting an operation. Return [HandlerStartOperationResultSync] to respond
	// successfully - inline, or [HandlerStartOperationResultAsync] to indicate that an asynchronous operation was
	// started. Return an [OperationError] to indicate that an operation completed as failed or
	// canceled.
	Start(ctx context.Context, input I, options StartOperationOptions) (HandlerStartOperationResult[O], error)
	// Cancel handles requests to cancel an asynchronous operation.
	// Cancelation in Nexus is:
	//  1. asynchronous - returning from this method only ensures that cancelation is delivered, it may later be
	//  ignored by the underlying operation implemention.
	//  2. idempotent - implementors should ignore duplicate cancelations for the same operation.
	Cancel(ctx context.Context, token string, options CancelOperationOptions) error

	mustEmbedUnimplementedOperation()
}

type syncOperation[I, O any] struct {
	UnimplementedOperation[I, O]

	handler func(context.Context, I, StartOperationOptions) (O, error)
	name    string
}

// NewSyncOperation is a helper for creating a synchronous-only [Operation] from a given name and handler function.
func NewSyncOperation[I, O any](name string, handler func(ctx context.Context, input I, options StartOperationOptions) (O, error)) Operation[I, O] {
	return &syncOperation[I, O]{
		name:    name,
		handler: handler,
	}
}

// Name implements Operation.
func (h *syncOperation[I, O]) Name() string {
	return h.name
}

// Start implements Operation.
func (h *syncOperation[I, O]) Start(ctx context.Context, input I, options StartOperationOptions) (HandlerStartOperationResult[O], error) {
	o, err := h.handler(ctx, input, options)
	if err != nil {
		return nil, err
	}
	return &HandlerStartOperationResultSync[O]{Value: o}, err
}

// A Service is a container for a group of operations.
type Service struct {
	Name string

	mu         sync.RWMutex
	operations map[string]RegisterableOperation
	dynamic    OperationHandler[*LazyValue, any]
}

// NewService constructs a [Service].
func NewService(name string) *Service {
	return &Service{
		Name:       name,
		operations: make(map[string]RegisterableOperation),
	}
}

// Register one or more operations.
// Returns an error if duplicate operations were registered with the same name or when trying to register an operation
// with no name.
//
// Can be called while requests are being handled. Existing requests keep their captured handler.
func (s *Service) Register(operations ...RegisterableOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dups []string
	for _, op := range operations {
		if op.Name() == "" {
			return fmt.Errorf("tried to register an operation with no name")
		}
		if _, found := s.operations[op.Name()]; found {
			dups = append(dups, op.Name())
		} else {
			s.operations[op.Name()] = op
		}
	}
	if len(dups) > 0 {
		return fmt.Errorf("duplicate operations: %s", strings.Join(dups, ", "))
	}
	return nil
}

// MustRegister registers one or more operations.
// Panics if duplicate operations were registered with the same name or when trying to register an operation with no
// name.
//
// Can be called while requests are being handled. Existing requests keep their captured handler.
func (s *Service) MustRegister(operations ...RegisterableOperation) {
	if err := s.Register(operations...); err != nil {
		panic(err)
	}
}

// Operation returns an operation by name or nil if not found.
func (s *Service) Operation(name string) RegisterableOperation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.operations[name]
}

// Update replaces registered operations. It fails without changes if a name is absent.
// Requests that have captured a handler continue using that handler.
func (s *Service) Update(operations ...RegisterableOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range operations {
		if op.Name() == "" || s.operations[op.Name()] == nil {
			return fmt.Errorf("cannot update unregistered operation %q", op.Name())
		}
	}
	for _, op := range operations {
		s.operations[op.Name()] = op
	}
	return nil
}

// Delete removes registered operations immediately. It fails without changes if a name is absent.
// Running asynchronous executions continue independently of the service registry.
func (s *Service) Delete(names ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		if s.operations[name] == nil {
			return fmt.Errorf("cannot delete unregistered operation %q", name)
		}
	}
	for _, name := range names {
		delete(s.operations, name)
	}
	return nil
}

// RegisterDynamicOperation registers the single fallback for unknown start and cancel names.
// Start receives the raw LazyValue and must consume it before returning. The requested
// operation name is available through ExtractHandlerInfo. Named operations take precedence.
// The handler may be invoked concurrently and must be safe for concurrent use.
func (s *Service) RegisterDynamicOperation(handler OperationHandler[*LazyValue, any]) error {
	if handler == nil {
		return errors.New("dynamic operation handler must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dynamic != nil {
		return errors.New("dynamic operation handler already registered")
	}
	s.dynamic = handler
	return nil
}

// DynamicOperation returns the fallback handler, or nil if none is registered.
func (s *Service) DynamicOperation() OperationHandler[*LazyValue, any] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dynamic
}

func (s *Service) resolve(name string) (RegisterableOperation, OperationHandler[*LazyValue, any]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if op := s.operations[name]; op != nil {
		return op, nil
	}
	return nil, s.dynamic
}

// MiddlewareFunc is a function which receives an OperationHandler and returns another OperationHandler.
// If the middleware wants to stop the chain before any handler is called, it can return an error.
//
// To get [HandlerInfo] for the current handler, call [ExtractHandlerInfo] with the given context.
//
// NOTE: Experimental
type MiddlewareFunc func(ctx context.Context, next OperationHandler[any, any]) (OperationHandler[any, any], error)

// A ServiceRegistry registers services and constructs a [Handler] that dispatches operations requests to those services.
type ServiceRegistry struct {
	services       map[string]*Service
	middleware     []MiddlewareFunc
	cancelFallback OperationHandler[any, any]
}

// NewServiceRegistry constructs an empty [ServiceRegistry].
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		services:   make(map[string]*Service),
		middleware: make([]MiddlewareFunc, 0),
	}
}

// Register one or more service.
// Returns an error if duplicate operations were registered with the same name or when trying to register a service with
// no name.
//
// Can be called multiple times and is not thread safe.
func (r *ServiceRegistry) Register(services ...*Service) error {
	var dups []string
	for _, service := range services {
		if service.Name == "" {
			return fmt.Errorf("tried to register a service with no name")
		}
		if _, found := r.services[service.Name]; found {
			dups = append(dups, service.Name)
		} else {
			r.services[service.Name] = service
		}
	}
	if len(dups) > 0 {
		return fmt.Errorf("duplicate services: %s", strings.Join(dups, ", "))
	}
	return nil
}

// Register one or more service.
// Panics if duplicate operations were registered with the same name or when trying to register a service with no name.
//
// Can be called multiple times and is not thread safe.
func (r *ServiceRegistry) MustRegister(services ...*Service) {
	if err := r.Register(services...); err != nil {
		panic(err)
	}
}

// Use registers one or more middleware to be applied to all operation method invocations across all registered
// services. Middleware is applied in registration order. If called multiple times, newly registered middleware will be
// applied after any previously registered ones.
//
// NOTE: Experimental
func (s *ServiceRegistry) Use(middleware ...MiddlewareFunc) {
	s.middleware = append(s.middleware, middleware...)
}

// SetCancelFallback sets a handler for cancellation of unknown operation names in
// registered services. Named and dynamic handlers take precedence. Call this before
// NewHandler. Frameworks can use it to cancel backing executions after operation removal.
func (r *ServiceRegistry) SetCancelFallback(handler OperationHandler[any, any]) {
	r.cancelFallback = handler
}

// NewHandler creates a [Handler] that dispatches requests to registered operations based on their name.
func (r *ServiceRegistry) NewHandler() (Handler, error) {
	if len(r.services) == 0 {
		return nil, errors.New("must register at least one service")
	}
	for _, service := range r.services {
		service.mu.RLock()
		empty := len(service.operations) == 0 && service.dynamic == nil
		service.mu.RUnlock()
		if empty {
			return nil, fmt.Errorf("service %q has no operations registered", service.Name)
		}
	}

	return &registryHandler{services: r.services, middlewares: r.middleware, cancelFallback: r.cancelFallback}, nil
}

type registryHandler struct {
	UnimplementedHandler

	services       map[string]*Service
	middlewares    []MiddlewareFunc
	cancelFallback OperationHandler[any, any]
}

func (r *registryHandler) wrapHandler(ctx context.Context, handler OperationHandler[any, any]) (OperationHandler[any, any], error) {
	for i := len(r.middlewares) - 1; i >= 0; i-- {
		var err error
		handler, err = r.middlewares[i](ctx, handler)
		if err != nil {
			return nil, err
		}
	}
	return handler, nil
}

func (r *registryHandler) resolve(service, operation string) (RegisterableOperation, OperationHandler[*LazyValue, any], error) {
	s := r.services[service]
	if s == nil {
		return nil, nil, NewHandlerErrorf(HandlerErrorTypeNotFound, "service %q not found", service)
	}
	op, dynamic := s.resolve(operation)
	return op, dynamic, nil
}

// CancelOperation implements Handler.
func (r *registryHandler) CancelOperation(ctx context.Context, service, operation, token string, options CancelOperationOptions) error {
	op, dynamic, err := r.resolve(service, operation)
	if err != nil {
		return err
	}
	var handler OperationHandler[any, any]
	switch {
	case op != nil:
		handler = &rootOperationHandler{h: op}
	case dynamic != nil:
		handler = &dynamicOperationHandler{handler: dynamic}
	case r.cancelFallback != nil:
		handler = r.cancelFallback
	default:
		return NewHandlerErrorf(HandlerErrorTypeNotFound, "operation %q not found", operation)
	}
	handler, err = r.wrapHandler(ctx, handler)
	if err != nil {
		return err
	}
	return handler.Cancel(ctx, token, options)
}

// StartOperation implements Handler.
func (r *registryHandler) StartOperation(ctx context.Context, service, operation string, input *LazyValue, options StartOperationOptions) (HandlerStartOperationResult[any], error) {
	op, dynamic, err := r.resolve(service, operation)
	if err != nil {
		return nil, err
	}
	var handler OperationHandler[any, any]
	var value any
	switch {
	case op != nil:
		handler = &rootOperationHandler{h: op}
	case dynamic != nil:
		handler = &dynamicOperationHandler{handler: dynamic}
	default:
		return nil, NewHandlerErrorf(HandlerErrorTypeNotFound, "operation %q not found", operation)
	}
	handler, err = r.wrapHandler(ctx, handler)
	if err != nil {
		return nil, err
	}
	if dynamic != nil {
		value = input
	} else {
		m, _ := reflect.TypeOf(op).MethodByName("Start")
		iptr := reflect.New(m.Type.In(2)).Interface()
		if err := input.Consume(iptr); err != nil {
			return nil, err
		}
		value = reflect.ValueOf(iptr).Elem().Interface()
	}
	return handler.Start(ctx, value, options)
}

type dynamicOperationHandler struct {
	UnimplementedOperation[any, any]
	handler OperationHandler[*LazyValue, any]
}

func (h *dynamicOperationHandler) Start(ctx context.Context, input any, options StartOperationOptions) (HandlerStartOperationResult[any], error) {
	return h.handler.Start(ctx, input.(*LazyValue), options)
}

func (h *dynamicOperationHandler) Cancel(ctx context.Context, token string, options CancelOperationOptions) error {
	return h.handler.Cancel(ctx, token, options)
}

type rootOperationHandler struct {
	UnimplementedOperation[any, any]
	h RegisterableOperation
}

func (r *rootOperationHandler) Cancel(ctx context.Context, token string, options CancelOperationOptions) error {
	// NOTE: We could avoid reflection here if we put the Cancel method on RegisterableOperation but it doesn't seem
	// worth it since we need reflection for the generic methods.
	m, _ := reflect.TypeOf(r.h).MethodByName("Cancel")
	values := m.Func.Call([]reflect.Value{reflect.ValueOf(r.h), reflect.ValueOf(ctx), reflect.ValueOf(token), reflect.ValueOf(options)})
	if values[0].IsNil() {
		return nil
	}
	return values[0].Interface().(error)
}

func (r *rootOperationHandler) Start(ctx context.Context, input any, options StartOperationOptions) (HandlerStartOperationResult[any], error) {
	m, _ := reflect.TypeOf(r.h).MethodByName("Start")
	values := m.Func.Call([]reflect.Value{reflect.ValueOf(r.h), reflect.ValueOf(ctx), reflect.ValueOf(input), reflect.ValueOf(options)})
	if !values[1].IsNil() {
		return nil, values[1].Interface().(error)
	}
	ret := values[0].Interface()
	return ret.(HandlerStartOperationResult[any]), nil
}
