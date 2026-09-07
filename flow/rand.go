package flow

import (
	"math/rand"
	"uuid"
)

// Rand returns a source of random numbers whose seed is drawn once and
// recorded, so a replay reproduces the same sequence. Draw from it as usual;
// the draws are not recorded, only the seed, so the same body yields the same
// numbers on every attempt. Use it instead of the math/rand global inside a
// run. Each call returns an independent source with its own recorded seed.
func (c Context) Rand() (*rand.Rand, error) {
	seed, err := c.Effect(func() (int64, error) { return int64(rand.Uint64()), nil })
	if err != nil {
		return nil, err
	}
	return rand.New(rand.NewSource(seed)), nil
}

// UUID returns a new random UUID, recorded so a replay returns the same one.
// Use it instead of uuid.New inside a run.
func (c Context) UUID() (string, error) {
	return c.Effect(func() (string, error) { return uuid.New().String(), nil })
}
