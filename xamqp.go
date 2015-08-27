package xamqp

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/streadway/amqp"
)

type Q struct {
	c     *amqp.Connection
	Close chan *amqp.Error
}

type Session struct {
	Exchange string
	Name     string
	Key      string
}

func Dial(s string) (*Q, error) {
	c, err := amqp.Dial(s)
	if err != nil {
		fmt.Println("xamqp couldn't connect")
		return nil, err
	}
	q := &Q{
		c:     c,
		Close: make(chan *amqp.Error, 0),
	}
	fmt.Println("xamqp connected")
	go func() {
		amqpErrC := make(chan *amqp.Error, 0)
		q.c.NotifyClose(amqpErrC)
		amqpErr := <-amqpErrC
		fmt.Println("xamqp connection lost")
		q.Close <- amqpErr
	}()
	return q, nil
}

type Channel struct {
	c *amqp.Channel
	//stop chan *amqp.Error
}

func (q *Q) Channel() (*Channel, error) {
	c, err := q.c.Channel()
	if err != nil {
		return nil, err
	}
	ch := &Channel{
		c: c,
		//	stop: make(chan *amqp.Error, 0),
	}
	//ch.c.NotifyClose(ch.stop)
	return ch, nil
}

func (c *Channel) ApplySession(ses *Session) error {
	var err error

	if err = c.c.ExchangeDeclare(
		ses.Exchange, // name of the exchange
		"fanout",     // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		amqp.Table{}, // arguments
	); err != nil {
		return err
	}

	_, err = c.c.QueueDeclare(
		ses.Name,     // name of the queue
		true,         // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // noWait
		amqp.Table{}, // arguments
	)
	if err != nil {
		return err
	}

	if err = c.c.QueueBind(
		ses.Name,     // name of the queue
		ses.Key,      // bindingKey
		ses.Exchange, // sourceExchange
		false,        // noWait
		amqp.Table{}, // arguments
	); err != nil {
		return err
	}

	return nil
}

func (c *Channel) Tx() error {
	return c.c.Tx()
}

func (c *Channel) Commit() error {
	return c.c.TxCommit()
}

func (c *Channel) Rollback() error {
	return c.c.TxRollback()
}

func (c *Channel) Publish(exchange, key string, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	m := amqp.Publishing{
		Body: data,
	}
	return c.c.Publish(exchange, key, false, false, m)
}

type Delivery struct {
	d amqp.Delivery
	*Channel
}

func (d Delivery) Ack() bool {
	if err := d.d.Ack(false); err != nil {
		return false
	}
	return true
}

func (d Delivery) Nack() {
	d.d.Ack(false)
}

func (c *Channel) Consume(name string, handler interface{}) error {
	dc, err := c.c.Consume(name, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}
	v := reflect.ValueOf(handler)
	mType := reflect.TypeOf(handler).In(0)
	go func() {
		for {
			select {
			case d, ok := <-dc:
				if !ok {
					return
				}
				msg := reflect.New(mType).Interface()
				err := json.Unmarshal(d.Body, msg)
				if err != nil {
					log.Printf("xamqp err: %s", err)
					continue
				}
				delivery := Delivery{
					d:       d,
					Channel: c,
				}
				in := []reflect.Value{reflect.ValueOf(msg).Elem(), reflect.ValueOf(delivery)}
				go func() { v.Call(in) }()
				// case <-c.stop:
				// 	fmt.Println(11)
				// 	return
			}
		}
	}()
	return nil
}

func (c *Channel) Qos(count int) error {
	return c.c.Qos(count, 0, false)
}

func IsConnectionError(err error) bool {
	amqpErr, ok := err.(*amqp.Error)
	if !ok {
		return false
	}
	switch amqpErr.Code {
	case 302, 501, 504:
		return true
	}
	return false
}
