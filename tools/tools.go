package tools

import (
	"fmt"
	"gopkg.in/inf.v0"
	"math"
	"math/rand"
	"reflect"
)

func FloatToDecimal(f interface{}) (*inf.Dec, bool) {
	typeOf := reflect.TypeOf(f).Kind()
	if typeOf != reflect.Float32 && typeOf != reflect.Float64 {
		return nil, false
	}

	d := new(inf.Dec)

	res, isOk := d.SetString(fmt.Sprint(f))
	return res, isOk
}

func RandFloat64(min, max float64) float64 {
	res := min + rand.Float64()*(max-min)
	return res
}

func RandDecimal(min, max *inf.Dec) *inf.Dec {
	diffScale := int64(max.Scale() - min.Scale())
	diffScaleAbs := math.Abs(float64(diffScale))
	maxScale := inf.Scale(math.Max(float64(min.Scale()), float64(max.Scale())))

	minUnscaled, _ := min.Unscaled()
	maxUnscaled, _ := max.Unscaled()

	if diffScale < 0 {
		maxUnscaled = maxUnscaled * int64(math.Pow(10, diffScaleAbs))
	} else if diffScale > 0 {
		minUnscaled = minUnscaled * int64(math.Pow(10, diffScaleAbs))
	}

	res := inf.NewDec(minUnscaled+rand.Int63n(maxUnscaled-minUnscaled), maxScale)
	return res
}

func FromStringToDec(s string) (*inf.Dec, bool) {
	d := new(inf.Dec)

	_, isOk := d.SetString(s)

	if !isOk {
		return nil, false
	}

	return d, true
}

func RandInt64(min, max int64) int64 {
	return int64(rand.Int63n(max-min+1) + min)
}
