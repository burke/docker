package fluentd

import (
	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/fluent/fluent-logger-golang/fluent"
	"strconv"
)

type Fluentd struct {
	tag           string
	containerid   string
	containername string
	writer        *fluent.Fluent
}

func New(config map[string]string, id string, name string) (logger.Logger, error) {
	port := 24224
	host := "localhost"
	if config["port"] != "" {
		port, _ = strconv.Atoi(config["port"])
	}
	if config["host"] != "" {
		host = config["host"]
	}
	tag := config["tag"]
	if tag == "" {
		tagPrefix := "docker."
		if config["tag-prefix"] != "" {
			tagPrefix = config["tag-prefix"] + "."
		}
		if config["tag-type"] == "name" {
			tag = tagPrefix + name
		} else if config["tag-type"] == "fullid" {
			tag = tagPrefix + id
		} else { // id
			tag = tagPrefix + id[:12]
		}
	}
	log, err := fluent.New(fluent.Config{FluentPort: port, FluentHost: host})
	logrus.Debugf("logging driver fluentd configured for container:%s.", id)
	logrus.Debugf("logging driver fluentd port:%d.", port)
	logrus.Debugf("logging driver fluentd host:%s.", host)
	logrus.Debugf("logging driver fluentd tag:%s.", tag)
	if err != nil {
		return nil, err
	}
	return &Fluentd{
		tag:           tag,
		containerid:   id,
		containername: name,
		writer:        log,
	}, nil
}

func (f *Fluentd) Log(msg *logger.Message) error {
	var data = map[string]string{
		"container_id":   f.containerid,
		"container_name": f.containername,
		"source":         msg.Source,
		"log":            string(msg.Line),
	}
	f.writer.PostWithTime(f.tag, msg.Timestamp, data)
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
