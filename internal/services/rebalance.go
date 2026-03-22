package services

import (
	"portfolio-rebalancer/internal/models"
	"time"

	"github.com/google/uuid"
)

func CalculateRebalance(current models.Portfolio, target models.Portfolio) ([]models.Transaction, error) {
	var sells []models.Transaction
	var buys []models.Transaction

	processAsset := func(assetName string, currentAmt, targetAmt int) error {
		drift := targetAmt - currentAmt
		if drift > 0 {
			newUUID, err := uuid.NewRandom()
			if err != nil {
				return err
			}

			// Target is higher: We need to BUY
			buys = append(buys, models.Transaction{
				ID:             newUUID.String(),
				UserID:         current.UserID,
				Type:           "BUY",
				AllocationType: assetName,
				Amount:         drift,
				CreatedAt:      time.Now().UTC(),
			})
		} else if drift < 0 {
			newUUID, err := uuid.NewRandom()
			if err != nil {
				return err
			}

			// Target is lower: We need to SELL
			sells = append(sells, models.Transaction{
				ID:             newUUID.String(),
				UserID:         current.UserID,
				Type:           "SELL",
				AllocationType: assetName,
				Amount:         -drift,
				CreatedAt:      time.Now().UTC(),
			})
		}
		return nil
	}

	if err := processAsset("stocks", current.Allocation.Stocks, target.Allocation.Stocks); err != nil {
		return nil, err
	}
	if err := processAsset("bonds", current.Allocation.Bonds, target.Allocation.Bonds); err != nil {
		return nil, err
	}
	if err := processAsset("gold", current.Allocation.Gold, target.Allocation.Gold); err != nil {
		return nil, err
	}

	transactions := append(sells, buys...)

	var order int = 1
	for i := range transactions {
		transactions[i].Order = order
		order++
	}

	return transactions, nil
}
