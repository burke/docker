package fluentd

import (
	"github.com/docker/docker/daemon/logger"
	"github.com/fluent/fluent-logger-golang/fluent"
)

type Fluentd struct {
	containerid string
	writer      *fluent.Fluent
}

func New(tag string) (logger.Logger, error) {
	log, err := fluent.New(fluent.Config{FluentPort: 24224, FluentHost: "localhost"})
	if err != nil {
		return nil, err
	}
	return &Fluentd{
		containerid: tag,
		writer:      log,
	}, nil
}

func (f *Fluentd) Log(msg *logger.Message) error {
	tag := "docker." + f.containerid + "." + msg.Source
	var data = map[string]string{
		"source": msg.Source,
		"log":    string(msg.Line),
	}
	f.writer.PostWithTime(tag, msg.Timestamp, data)
	// fluent-logger-golang buffers logs from failures and disconnections,
	// and these are transferred again automatically.
	return nil
}

func (f *Fluentd) Close() error {
	return f.writer.Close()
}

func (f *Fluentd) Name() string {
	return "Fluentd"
}
