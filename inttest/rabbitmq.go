// Package inttest provides setup functions that create a RabbitMQ container with toxiproxy. These
// help us test our consumer and producer under different network conditions like all connections
// being dropped. We are using the management image for RabbitMQ so you can debug and interact with
// tests using its admin panel. Use a debugger, adjust timeouts waiting for a message or add a
// time.Sleep and find the exposed management port to login to the UI. You will find it easier to
// debug if your test configures the consumers connection and or consumer tag prefix.
package inttest

import (
	"context"
	"fmt"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/docker/go-connections/nat"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SetupRabbitMQ creates a RabbitMQ and toxiproxy container with an AMQP client ready to send
// messages to it.
func SetupRabbitMQ(t *testing.T) *AMQP {
	t.Helper()
	require := require.New(t)
	ctx := context.TODO()

	net, err := network.New(ctx)
	require.NoError(err, "failed setting up Docker network")
	t.Cleanup(func() {
		require.NoError(net.Remove(ctx), "failed to remove the Docker network")
	})

	rabbitMQContainer, err := NewRabbitMQ(ctx, WithNetwork(net.Name, "rabbitmq"))
	require.NoError(err, "failed setting up RabbitMQ")
	t.Cleanup(func() {
		require.NoError(rabbitMQContainer.Terminate(ctx), "failed to terminate RabbitMQ")
	})

	proxiedPort := "18080" // port toxiproxy will listen on and forward to RabbitMQ
	natProxiedPort := proxiedPort + "/tcp"
	proxyContainer, err := NewToxiProxy(ctx, net.Name, WithExposedPorts(natProxiedPort))
	require.NoError(err, "failed setting up toxiproxy")
	t.Cleanup(func() {
		require.NoError(proxyContainer.Terminate(ctx), "failed to terminate toxiproxy")
	})

	toxi := toxiproxy.NewClient(proxyContainer.apiURI)
	proxy, err := toxi.CreateProxy("test_rabbitmq", "[::]:"+proxiedPort, rabbitMQContainer.networkAlias+":"+rabbitMQContainer.internalPort)
	require.NoError(err, "failed creating proxy in toxiproxy")

	exposedProxiedHost := "127.0.0.1"
	exposedProxiedPort, ok := proxyContainer.ports[natProxiedPort]
	require.True(ok, "failed to get RabbitMQ port proxied by toxiproxy")
	proxiedURI := amqpURI(rabbitMQContainer.user, rabbitMQContainer.pw, exposedProxiedHost, exposedProxiedPort)

	URI, err := rabbitMQContainer.AMQPURI(ctx)
	require.NoError(err, "failed to get RabbitMQ AMQP URI")
	conn, err := amqpgo.Dial(URI)
	require.NoError(err, "failed setting up AMQP connection")
	channel, err := conn.Channel()
	require.NoError(err, "failed setting up AMQP channel")

	return &AMQP{
		rabbitMQContainer: rabbitMQContainer,
		conn:              conn,
		Channel:           channel,
		Proxy:             proxy,
		ProxiedURI:        proxiedURI,
		proxiedPort:       exposedProxiedPort,
		proxiedHost:       exposedProxiedHost,
	}
}

// AMQP allows making requests to RabbitMQ either directly or via a proxy. It does so by opening a
// connection and channel to RabbitMQ directly via the low-level github.com/rabbitmq/amqp091-go
// library. Access the actual amqp091-go channel for specific use cases where our defaults don't
// work. Connect any code you want to test via the ProxiedURI if you want to test effects of
// connections being dropped via DisableConnections. If you want to test other scenarios relating
// networking issues use the Proxy and read the docs at http://github.com/Shopify/toxiproxy.
type AMQP struct {
	rabbitMQContainer *rabbitmqContainer
	conn              *amqpgo.Connection // Connection established directly with RabbitMQ without doing through the proxy.
	Channel           *amqpgo.Channel    // Channel established directly with RabbitMQ without doing through the proxy.
	Proxy             *toxiproxy.Proxy   // Proxy in front of RabbitMQ
	ProxiedURI        string             // Proxied AMQP URI going via toxiproxy to RabbitMQ. Use this if you want to test an implementation. Use URI if you want to consume/publish directly to RabbitMQ for example in an assertion.
	proxiedHost       string
	proxiedPort       string
}

// URI is the AMQP URI going to RabbitMQ directly without going through toxiproxy.
func (a *AMQP) URI(t *testing.T) string {
	t.Helper()

	URI, err := a.rabbitMQContainer.AMQPURI(context.TODO())
	require.NoError(t, err, "failed to get RabbitMQ URI")
	return URI
}

func (a *AMQP) GetProxiedURI(user, pw string) string {
	return amqpURI(user, pw, a.proxiedHost, a.proxiedPort)
}

// Publish a message to given queue. This will go directly to RabbitMQ using URI and thus not go via
// the proxy.
func (a *AMQP) Publish(t *testing.T, queue, message string) {
	t.Helper()

	err := a.Channel.PublishWithContext(context.TODO(), "", queue, false, false, amqpgo.Publishing{
		DeliveryMode: amqpgo.Persistent,
		Body:         []byte(message),
	})
	require.NoError(t, err, "failed to publish message to queue %q", queue)
}

// PublishEvery a message to given queue in given interval until done is closed. This will go
// directly to RabbitMQ using URI and thus not go via the proxy.
func (a *AMQP) PublishEvery(t *testing.T, tick time.Duration, done chan struct{}, queue, message string) {
	t.Helper()

	go func() {
		timer := time.NewTicker(tick)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
				a.Publish(t, queue, message)
			}
		}
	}()
}

// Enable connections to RabbitMQ via the proxy. This will affect anyone going through the
// ProxiedURI.
func (a *AMQP) EnableConnections(t *testing.T) {
	t.Helper()

	require.NoError(t, a.Proxy.Enable(), "failed to enable proxy to RabbitMQ")
}

// Disable connections to RabbitMQ via the proxy. This will affect anyone going through the
// ProxiedURI. It will drop existing connections and prevent new connections until
// EnableConnections() is called.
func (a *AMQP) DisableConnections(t *testing.T) {
	t.Helper()

	require.NoError(t, a.Proxy.Disable(), "failed to disable proxy to RabbitMQ")
}

func (a *AMQP) Restart(t *testing.T) {
	t.Helper()

	require.NoError(t, a.rabbitMQContainer.Stop(context.TODO(), nil), "failed to stop RabbitMQ")
	require.NoError(t, a.rabbitMQContainer.Start(context.TODO()), "failed to start RabbitMQ")

	URI, err := a.rabbitMQContainer.AMQPURI(context.TODO())
	require.NoError(t, err, "failed to get RabbitMQ AMQP URI")
	conn, err := amqpgo.Dial(URI)
	require.NoError(t, err, "failed setting up AMQP connection")
	a.conn = conn
	channel, err := conn.Channel()
	require.NoError(t, err, "failed setting up AMQP channel")
	a.Channel = channel

	t.Logf("Restarted RabbitMQ with new connection and channel for publishing to unproxied URI %q", URI)
}

type rabbitmqContainer struct {
	testcontainers.Container
	user         string
	pw           string
	network      string
	networkAlias string
	internalPort string
}

func (rc *rabbitmqContainer) AMQPURI(ctx context.Context) (string, error) {
	ip, err := rc.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := rc.ExposedAMQPPort(ctx)
	if err != nil {
		return "", err
	}
	return amqpURI(rc.user, rc.pw, ip, port), nil
}

func (rc *rabbitmqContainer) ExposedAMQPPort(ctx context.Context) (string, error) {
	port, err := rc.MappedPort(ctx, nat.Port("5672/tcp"))
	if err != nil {
		return "", err
	}
	return port.Port(), nil
}

type rabbitMQOptions struct {
	user         string
	pw           string
	network      string
	networkAlias string
}

type rabbitMQOption func(*rabbitMQOptions)

func WithUser(user string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.user = user
	}
}

func WithPassword(pw string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.pw = pw
	}
}

// WithNetwork connects the RabbitMQ container to a specific network and gives it an alias with
// which you can reach it on this network.
func WithNetwork(name, alias string) rabbitMQOption {
	return func(options *rabbitMQOptions) {
		options.network = name
		options.networkAlias = alias
	}
}

// NewRabbitMQ creates a RabbitMQ container. The container will be listening and ready to accept
// connections. Connect using default user and password rabbitmq or the credentials you provided via
// the options.
func NewRabbitMQ(ctx context.Context, options ...rabbitMQOption) (*rabbitmqContainer, error) {
	opts := &rabbitMQOptions{
		user: "rabbitmq",
		pw:   "rabbitmq",
	}
	for _, o := range options {
		o(opts)
	}

	port := "5672"
	natPort := port + "/tcp"
	natPortMgmt := "15672/tcp"
	req := testcontainers.ContainerRequest{
		Image: "rabbitmq:3.8.9-management",
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": opts.user,
			"RABBITMQ_DEFAULT_PASS": opts.pw,
		},
		ExposedPorts: []string{natPort, natPortMgmt},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(nat.Port(natPort)),
			WaitForRabbitMQ(opts.user, opts.pw, natPort),
		).WithDeadline(time.Minute),
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

	return &rabbitmqContainer{
		Container:    container,
		network:      opts.network,
		networkAlias: opts.networkAlias,
		internalPort: port,
		user:         opts.user,
		pw:           opts.pw,
	}, nil
}

func amqpURI(user, pw, ip, port string) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", user, pw, ip, port)
}

// rabbitMQstrategy implements testcontainers wait.Strategy to ensure RabbitMQ is up and we can
// connect to it using given credentials.
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

func (rbw *rabbitMQStrategy) connect(ip, port string) (*amqpgo.Connection, error) {
	uri := amqpURI(rbw.usr, rbw.pw, ip, port)
	fmt.Printf("Waiting for RabbitMQ connection to: %q\n", uri)
	return amqpgo.Dial(uri)
}

type toxiproxyContainer struct {
	testcontainers.Container
	apiURI string
	ports  map[string]string // ports to mapped ports
}

type toxiproxyOptions struct {
	exposedPorts []string
}

type toxiproxyOption func(*toxiproxyOptions)

func WithExposedPorts(ports ...string) toxiproxyOption {
	return func(options *toxiproxyOptions) {
		options.exposedPorts = ports
	}
}

// NewToxiProxy creates a toxiproxy container.
func NewToxiProxy(ctx context.Context, network string, options ...toxiproxyOption) (*toxiproxyContainer, error) {
	opts := &toxiproxyOptions{}
	for _, o := range options {
		o(opts)
	}

	natAPIPort := "8474/tcp" // toxiproxy API
	exposedPorts := []string{natAPIPort}
	exposedPorts = append(exposedPorts, opts.exposedPorts...)

	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/shopify/toxiproxy",
		ExposedPorts: exposedPorts,
		WaitingFor:   wait.ForHTTP("/version").WithPort(nat.Port(natAPIPort)),
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
	rp, err := container.MappedPort(ctx, nat.Port(natAPIPort))
	if err != nil {
		return nil, err
	}
	apiURI := fmt.Sprintf("http://%s:%s", ip, rp.Port())

	ports := make(map[string]string)
	for _, p := range opts.exposedPorts {
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
