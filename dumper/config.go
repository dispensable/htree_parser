package dumper

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type DumperCfg struct {
	Analysis struct {
		FieldREs []string `yaml:"fieldsREs"`
		KeyREs []string `yaml:"keyREs"`
		SeperatorRE string `yaml:"sepRE"`
		ReplacedTo string `yaml:"replacedTo"`
	} `yaml:"analysis"`
	Dumper struct {
		KeyPatterns []string `yaml:"keyPatterns"`
	} `yaml:"dumper"`
	TR struct {
		KeyPatterns []string `yaml:"keyPatterns"`
	} `yaml:"tr"`
}

func NewDumperCfgFromFile(cfgPath string) (*DumperCfg, error) {
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("read cfg file %s err: %s", cfgPath, err)
	}

	dumperCfg := DumperCfg{}
	err = yaml.Unmarshal(data, &dumperCfg)
	if err != nil {
		return nil, fmt.Errorf("parse cfg %s file err: %s", cfgPath, err)
	}

	log.Debugf("create config: %+v", dumperCfg)
	return &dumperCfg, nil
}
