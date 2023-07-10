package dumper
import (
	"bytes"
	"fmt"

	"math/rand"

	golibmc "github.com/douban/libmc/src"
)

func stGetRSet(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	item, err := fromFinder.client.Get(key)
	if item == nil {
		return nil
	}

	if err != nil {
		return fmt.Errorf("get from finder err: %s", err)
	}

	item.Key = fmt.Sprintf("%d%s", len(item.Key), item.Key)
	if prod {
		err = toFinder.client.Set(item)
		if err != nil {
			return fmt.Errorf("set to db err: %s", err)
		}
	} else {
		log.Infof("[DRY RUN] will set key %s to %s", key, item.Value)
	}
	return nil
}

func stGetSet(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	item, err := fromFinder.client.Get(key)
	if item == nil {
		return nil
	}

	if err != nil {
		return fmt.Errorf("get from finder err: %s", err)
	}

	if prod {
		err = toFinder.client.Set(item)
		if err != nil {
			return fmt.Errorf("set to db err: %s", err)
		}
	} else {
		log.Infof("[DRY RUN] will set key %s to %s", key, item.Value)
	}
	return nil
}

func stGet(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	item, err := toFinder.client.Get(key)
	if item == nil {
		return nil
	}
	return err
}

func stRGet(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	item, err := toFinder.client.Get(fmt.Sprintf("%d%s", len(key), key))
	if item == nil {
		return nil
	}
	return err
}

func stGetRCmp(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	key = fmt.Sprintf("%d%s", len(key), key)
	itemF, errf := fromFinder.client.Get(key)
	itemT, errt := toFinder.client.Get(key)

	if errf != nil {
		return fmt.Errorf("get from keyfinder err: %s", errf)
	}

	if errt != nil {
		return fmt.Errorf("get from tofinder err: %s", errt)
	}
	
	if itemF == nil && itemT == nil {
		return nil
	}

	if itemF == nil || itemT == nil {
		return fmt.Errorf("itemF: %v itemT: %v | not all nil", itemF, itemT)
	}

	if bytes.Compare(itemF.Value, itemT.Value) == 0 {
		return nil
	}

	return fmt.Errorf("itemF and itemT not equal: %v || %v", itemF, itemT)
}

func stGetCmp(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	itemF, errf := fromFinder.client.Get(key)
	itemT, errt := toFinder.client.Get(key)

	if errf != nil {
		return fmt.Errorf("get from keyfinder err: %s", errf)
	}

	if errt != nil {
		return fmt.Errorf("get from tofinder err: %s", errt)
	}
	
	if itemF == nil && itemT == nil {
		return nil
	}

	if itemF == nil || itemT == nil {
		return fmt.Errorf("itemF: %v itemT: %v | not all nil", itemF, itemT)
	}

	if bytes.Compare(itemF.Value, itemT.Value) == 0 {
		return nil
	}

	return fmt.Errorf("itemF and itemT not equal: %v || %v", itemF, itemT)
}

func stSync(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	itemF, errf := fromFinder.client.Get(key)
	itemT, errt := toFinder.client.Get(key)

	if errf != nil {
		return fmt.Errorf("get from keyfinder err: %s", errf)
	}

	if errt != nil {
		return fmt.Errorf("get from tofinder err: %s", errt)
	}

	if itemF == nil && itemT == nil {
		return nil
	}

	if itemF == nil || itemT == nil {
		if itemF == nil {
			if prod {
				err := toFinder.client.Delete(key)
				if err != nil {
					return fmt.Errorf("delete key from to finder err: %s", err)
				}
				return err
			} else {
				log.Infof("[DRY RUN] will delete key from to finder: %s", key)
			}
		} else {
			if prod {
				err := toFinder.client.Set(itemF)
				if err != nil {
					return fmt.Errorf("set key to finder err: %s", err)
				}
				return err
			} else {
				log.Infof("[DRY RUN] will set key %s to %s", key, itemF.Value)
			}
		}
	}

	if bytes.Compare(itemF.Value, itemT.Value) == 0 {
		return nil
	}

	if prod {
		err := toFinder.client.Set(itemF)
		if err != nil {
			return fmt.Errorf("set key to finder err: %s", err)
		}
		return err
	} else {
		log.Infof("[DRY RUN] will set key %s to %s", key, itemF.Value)
	}
	return nil
}

func stDeleteR(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	// _ = toFinder
	if prod {
		return fromFinder.client.Delete(fmt.Sprintf("%d%s", len(key), key))
	} else {
		log.Infof("[DRY RUN] will delete key: %s", key)
	}
	return nil
}

func stDelete(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	// _ = toFinder
	if prod {
		return fromFinder.client.Delete(key)
	} else {
		log.Infof("[DRY RUN] will delete key: %s", key)
	}
	return nil
}

func getFuncByRW(r int, randKey bool) (stFuncT, error) {
	if !(r < 10) {
		return nil, fmt.Errorf("r must < 10")
	}

	if !randKey {
		return func(key string, fkf, tkf *KeyFinder, prod bool) error {
			if rand.Intn(10) < r {
				return stGet(key, fkf, tkf, prod)
			} else {
				return stGetSet(key, fkf, tkf, prod)
			}
		}, nil
	} else {
		return func(key string, fkf, tkf *KeyFinder, prod bool) error {
			if rand.Intn(10) < r {
				return stRGet(key, fkf, tkf, prod)
			} else {
				return stGetRSet(key, fkf, tkf, prod)
			}
		}, nil
	}
}

// set mockdata to db
func setRandomR(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	key = fmt.Sprintf("%d%s", len(key), key)
	randData := RandStringBytesMaskImprSrc(256)
	if prod {
		item := new(golibmc.Item)
		item.Key = key
		item.Value = randData
		return fromFinder.client.Set(item)
	} else {
		log.Infof("[DRY RUN] will set key %s to %s", key , randData)
	}
	return nil
}

// set mockdata to db
func setRandom(key string, fromFinder, toFinder *KeyFinder, prod bool) error {
	randData := RandStringBytesMaskImprSrc(256)
	if prod {
		item := new(golibmc.Item)
		item.Key = key
		item.Value = randData
		return fromFinder.client.Set(item)
	} else {
		log.Infof("[DRY RUN] will set key %s to %s", key , randData)
	}
	return nil
}
