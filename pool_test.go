package gcopool

import (
	"context"
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		pool, err := New(Config{
			CreateResource: func(ctx context.Context) (res Resource, err error) {
				return NewR(), nil
			},
		})
		if err != nil {
			t.Fatal("create pool: " + err.Error())
		}
		if !pool.IsValid() {
			t.Error("pool must be valid")
		}
		pool.Close()
	})
}

type res struct {
	id string
	tx TXID
}

func (r res) ID() string {
	return r.id
}

// TODO: no id
func (r *res) Ping(context.Context, string) error {
	return nil
}

// TODO: no id, how to release write mode
func (r *res) Prepare(context.Context, string) (TXID, error) {
	r.tx = true
	return r.tx, nil
}

// TODO: no id
func (r *res) Destroy(context.Context, string) error {
	if r.id == "" {
		return errors.New("already destroyed")
	}
	r.id = ""
	return nil
}

func NewR() Resource {
	return &res{}
}
