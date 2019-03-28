package gcopool

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		id := 0
		pool, err := New(Config{
			CreateResource: func(ctx context.Context) (res Resource, err error) {
				id++
				return NewR(strconv.Itoa(id)), nil
			},
		})
		if err != nil {
			t.Fatal("create pool: " + err.Error())
		}
		if !pool.IsValid() {
			t.Error("pool must be valid")
		}
		for i := 0; i < 100; i++ {
			h, err := pool.Take(context.Background())
			if err != nil {
				t.Error(err)
				continue
			}
			if h.GetID() != "1" {
				t.Error("leaked resource")
			}
			h.Recycle()
		}
		for i := 0; i < 100; i++ {
			h, err := pool.Take(context.Background())
			if err != nil {
				t.Error(err)
				continue
			}
			if h.GetID() != strconv.Itoa(i+1) {
				t.Error("unexpected limit")
			}
		}
		pool.Close()
	})

	t.Run("max=2", func(t *testing.T) {
		id := 0
		pool, err := New(Config{
			MaxOpened: 2,
			CreateResource: func(ctx context.Context) (res Resource, err error) {
				id++
				return NewR(strconv.Itoa(id)), nil
			},
		})
		if err != nil {
			t.Fatal("create pool: " + err.Error())
		}
		if !pool.IsValid() {
			t.Error("pool must be valid")
		}
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := pool.Take(ctx)
			cancel()
			if err != nil {
				if i == 2 && err == ErrGetSessionTimeout {
					break
				}
				t.Error(err)
				continue
			}
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

func (r *res) Ping(context.Context) error {
	return nil
}

func (r *res) Prepare(context.Context) (TXID, error) {
	r.tx = true
	return r.tx, nil
}

func (r *res) Destroy(context.Context) error {
	if r.id == "" {
		return errors.New("already destroyed")
	}
	r.id = ""
	return nil
}

func NewR(id string) Resource {
	return &res{
		id: id,
	}
}
