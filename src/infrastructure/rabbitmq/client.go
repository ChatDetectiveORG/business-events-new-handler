package rabbitmq

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Client is a minimal RabbitMQ connection holder.
//
// Notes:
// - AMQP channels are NOT safe for concurrent use; always take a fresh channel per goroutine/work unit.
// - Connection recovery/reconnect can be added later; the interface below keeps it contained.
type Client struct {
	url string
	cfg amqp.Config

	mu   sync.Mutex
	conn *amqp.Connection
}

func NewClient(url string, cfg amqp.Config) (*Client, error) {
	c := &Client{url: url, cfg: cfg}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}

	conn, err := amqp.DialConfig(c.url, c.cfg)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *Client) Conn() (*amqp.Connection, error) {
	if err := c.connect(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn, nil
}

// Channel opens a new channel on the current connection.
func (c *Client) Channel() (*amqp.Channel, error) {
	conn, err := c.Conn()
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	if c.conn.IsClosed() {
		return nil
	}
	return c.conn.Close()
}
