package models

import (
	"errors"
	"fmt"
)

func ValidateAllocation(a Allocation) error {
	if a.Stocks < 0 || a.Bonds < 0 || a.Gold < 0 {
		return errors.New("allocation percents must be non-negative")
	}
	sum := a.Stocks + a.Bonds + a.Gold
	if sum != 100 {
		return fmt.Errorf("allocation must sum to 100 (got %d)", sum)
	}
	return nil
}
