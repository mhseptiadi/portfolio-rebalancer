package models

import (
	"errors"
	"fmt"
	"math"
)

func ValidateAllocation(a Allocation) error {
	if a.Stocks < 0 || a.Bonds < 0 || a.Gold < 0 {
		return errors.New("allocation percents must be non-negative")
	}
	sum := a.Stocks + a.Bonds + a.Gold
	if math.Abs(sum-100.0) > 1e-9 {
		return fmt.Errorf("allocation must sum to 100 (got %f)", sum)
	}
	return nil
}
