package main

type Config struct {
	NodeID      string `env:"NODE_ID"`
	Region      string `env:"NODE_REGION"`
	ListenAddr  string `env:"LISTEN_ADDR"`
	ContactID   string `env:"CONTACT_NODE_ID"`
	ContactAddr string `env:"CONTACT_NODE_ADDR"`
	TAgg        string `env:"T_AGG"`
	K           int    `env:"K"`
	EpochLength int    `env:"EPOCH_LENGTH"`
	MinNoNews   int    `env:"MIN_NO_NEWS"`
}
