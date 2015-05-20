package fluentd

import (
	"io"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/fluent/fluent-logger-golang/fluent"
)

type Fluentd struct {
	tag           string
	containerID   string
	containerName string
	writer        *fluent.Fluent
}

const name = "fluentd"

const defaultHostName = "localhost"
const defaultPort = 24224
const defaultTagPrefix = "docker"

func init() {
	if err := logger.RegisterLogDriver(name, New); err != nil {
		logrus.Fatal(err)
	}
}

func New(ctx logger.Context) (logger.Logger, error) {
	config := ctx.Config
	port := defaultPort
	host := defaultHostName
	if config["port"] != "" {
		portp, errp := strconv.Atoi(config["port"])
		if errp != nil {
			return nil, errp
		}
		port = portp
	}
	if config["host"] != "" {
		host = config["host"]
	}
	tag := config["tag"]
	if tag == "" {
		tagPrefix := defaultTagPrefix
		if config["tag-prefix"] != "" {
			tagPrefix = config["tag-prefix"]
		}
		switch config["tag-type"] {
		case "name":
			tag = tagPrefix + "." + ctx.ContainerName
		case "fullid":
			tag = tagPrefix + "." + ctx.ContainerID
		default:
			tag = tagPrefix + "." + ctx.ContainerID[:12]
		}
	}
	log, err := fluent.New(fluent.Config{FluentPort: port, FluentHost: host})
	logrus.Debugf("logging driver fluentd configured for container:%s.", ctx.ContainerID)
	logrus.Debugf("logging driver fluentd port:%d.", port)
	logrus.Debugf("logging driver fluentd host:%s.", host)
	logrus.Debugf("logging driver fluentd tag:%s.", tag)
	if err != nil {
		return nil, err
	}
	return &Fluentd{
		tag:           tag,
		containerID:   ctx.ContainerID,
		containerName: ctx.ContainerName,
		writer:        log,
	}, nil
}

func (f *Fluentd) Log(msg *logger.Message) error {
	data := map[string]string{
		"container_id":   f.containerID,
		"container_name": f.containerName,
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
	return name
}

func (s *Fluentd) GetReader() (io.Reader, error) {
	return nil, logger.ReadLogsNotSupported
}
