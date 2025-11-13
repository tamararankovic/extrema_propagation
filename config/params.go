package config

import (
	"log"

	"github.com/caarlos0/env"
)

type Params struct {
	ID          string `env:"ID"`
	Tagg        int    `env:"T_AGG"        envDefault:"1"`
	Rmax        int    `env:"R_MAX"        envDefault:"3"`
	K           int    `env:"K"            envDefault:"400"`
	EpochLength int    `env:"EPOCH_LENGTH" envDefault:"15"`
	MinNoNews   int    `env:"MIN_NO_NEWS"  envDefault:"3"`
}

func LoadParamsFromEnv() Params {
	var p Params
	if err := env.Parse(&p); err != nil {
		log.Fatalln(err)
	}
	return p
}
