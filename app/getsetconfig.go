package app

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/util/log"
)

func getSetConfig(ctx context.Context, setCfg bool, data []byte) error {
	l := log.FromContext(ctx)
	ctx = log.WithLogger(ctx, l)
	if viper.GetBool("verbose") {
		l.EnableDebug()
	}

	err := viper.ReadInConfig()
	// ignore file not found error
	if err != nil && !isCfgNotFound(err) {
		return errors.Wrap(err, "read config")
	}

	c, err := getConfig(ctx)
	if err != nil {
		return err
	}
	ctx = permission.SystemContext(ctx, "SetConfig")
	return withStorage(ctx, c, func(ctx context.Context, s *deltastores.Stores) error {
		if setCfg {
			id, err := s.Config.SetConfigData(ctx, nil, data)
			if err != nil {
				return errors.Wrap(err, "save config")
			}
			log.Logf(ctx, "Saved config version %d", id)
			return nil
		}
		_, _, data, err := s.Config.ConfigData(ctx, nil)
		if err != nil {
			return errors.Wrap(err, "read config")
		}
		_, err = os.Stdout.Write(data)
		return err
	})
}
