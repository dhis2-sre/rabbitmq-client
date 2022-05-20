package rabbitmq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	amqp "github.com/rabbitmq/amqp091-go"
)

func setupNetwork(ctx context.Context, name string) (testcontainers.Network, error) {
	return testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name: name,
		},
	})
}

type rabbitMQStrategy struct {
	usr            string
	pw             string
	port           string
	startupTimeout time.Duration
}

func WaitForRabbitMQ(user, password, port string) *rabbitMQStrategy {
	return &rabbitMQStrategy{usr: user, pw: password, port: port, startupTimeout: time.Minute}
}

func (rbw *rabbitMQStrategy) WithStartupTimeout(timeout time.Duration) *rabbitMQStrategy {
	rbw.startupTimeout = timeout
	return rbw
}

func (rbw *rabbitMQStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) error {
	// limit context to startupTimeout
	ctx, cancelContext := context.WithTimeout(ctx, rbw.startupTimeout)
	defer cancelContext()

	ipAddress, err := target.Host(ctx)
	if err != nil {
		return nil
	}

	waitInterval := 50 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s:%w", ctx.Err(), err)
		case <-time.After(waitInterval):
			port, err := target.MappedPort(ctx, nat.Port(rbw.port))
			if err != nil {
				return fmt.Errorf("No mapped port for RabbitMQ found: %w", err)
			}
			conn, err := rbw.connect(ipAddress, port.Port())
			if err != nil {
				fmt.Printf("Connection to RabbitMQ failed: %s\n", err)
				continue
			}
			defer conn.Close()

			return nil
		}
	}
}

func (rbw *rabbitMQStrategy) connect(ip, port string) (*amqp.Connection, error) {
	uri := amqpURI(rbw.usr, rbw.pw, ip, port)
	fmt.Printf("Waiting for RabbitMQ connection to: %s\n", uri)
	return amqp.Dial(uri)
}

type rabbitmqContainer struct {
	testcontainers.Container
	usr          string
	pw           string
	network      string
	networkAlias string
	port         string
	amqpURI      string
}

type rabbitMQOptions struct {
	usr          string
	pw           string
	network      string
	networkAlias string
}

type rabbitMQOption func(*rabbitMQOptions)

func WithUser(user string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.usr = user
	}
}

func WithPassword(pw string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.pw = pw
	}
}

func WithNetwork(name, alias string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.network = name
		options.networkAlias = alias
	}
}

func NewRabbitMQ(ctx context.Context, options ...rabbitMQOption) (*rabbitmqContainer, error) {
	opts := &rabbitMQOptions{
		usr: "rabbitmq",
		pw:  "rabbitmq",
	}
	for _, o := range options {
		o(opts)
	}

	port := "5672"
	portC := port + "/tcp"
	portMgmtC := "15672/tcp"
	req := testcontainers.ContainerRequest{
		Image: "rabbitmq:3.8.9-management",
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": opts.usr,
			"RABBITMQ_DEFAULT_PASS": opts.pw,
		},
		ExposedPorts: []string{portC, portMgmtC},
		WaitingFor: wait.ForAll(
			// TODO do I need to wait until the port is ready?
			// or can my loop later on take care of that as well
			wait.ForListeningPort(nat.Port(portC)),
			WaitForRabbitMQ(opts.usr, opts.pw, portC),
		).WithStartupTimeout(time.Minute),
	}
	if opts.network != "" {
		req.Networks = []string{opts.network}
		req.NetworkAliases = map[string][]string{
			opts.network: {opts.networkAlias},
		}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	mp, err := container.MappedPort(ctx, nat.Port(portC))
	if err != nil {
		return nil, err
	}

	return &rabbitmqContainer{
		Container:    container,
		network:      opts.network,
		networkAlias: opts.networkAlias,
		port:         port,
		usr:          opts.usr,
		pw:           opts.pw,
		amqpURI:      amqpURI(opts.usr, opts.pw, ip, mp.Port()),
	}, nil
}

func amqpURI(usr, pw, ip, port string) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", usr, pw, ip, port)
}

type amqpTestClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func setupAMQPTestClient(URI string) (*amqpTestClient, error) {
	c, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	return &amqpTestClient{conn: c, ch: ch}, nil
}

type toxiproxyContainer struct {
	testcontainers.Container
	apiURI       string
	rabbitMQPort string
	ports        map[string]string // ports to mapped ports
}

type toxiproxyOptions struct {
	ports []string
}

type toxiproxyOption func(*toxiproxyOptions)

func WithPorts(ports ...string) toxiproxyOption {
	return func(options *toxiproxyOptions) {
		options.ports = ports
	}
}

// TODO make network alias configurable/expose it as with rabbitmq
func NewToxiProxy(ctx context.Context, network string, options ...toxiproxyOption) (*toxiproxyContainer, error) {
	opts := &toxiproxyOptions{}
	for _, o := range options {
		o(opts)
	}

	apiPort := "8474/tcp"
	exposedPorts := []string{apiPort}
	exposedPorts = append(exposedPorts, opts.ports...)

	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/shopify/toxiproxy",
		ExposedPorts: exposedPorts,
		WaitingFor:   wait.ForHTTP("/version").WithPort(nat.Port(apiPort)),
	}
	if network != "" {
		req.Networks = []string{network}
		req.NetworkAliases = map[string][]string{network: {"toxiproxy"}}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	rp, err := container.MappedPort(ctx, nat.Port(apiPort))
	if err != nil {
		return nil, err
	}
	apiURI := fmt.Sprintf("http://%s:%s", ip, rp.Port())

	ports := make(map[string]string)
	for _, p := range opts.ports {
		rp, err = container.MappedPort(ctx, nat.Port(p))
		if err != nil {
			return nil, err
		}
		ports[p] = rp.Port()
	}

	return &toxiproxyContainer{
		Container: container,
		apiURI:    apiURI,
		ports:     ports,
	}, nil
}
