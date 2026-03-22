package models

import "time"

type Allocation struct {
	Stocks int `json:"stocks"`
	Bonds  int `json:"bonds"`
	Gold   int `json:"gold"`
}

type Portfolio struct {
	UserID     string     `json:"user_id"`
	Allocation Allocation `json:"allocation"` // Current user allocation in percentage terms
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type UpdatedPortfolio struct {
	UserID        string     `json:"user_id"`
	NewAllocation Allocation `json:"new_allocation"` // Updated user allocation from provider in percentage terms
}

type Transaction struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	Type           string    `json:"type"` // e.g. "sell", "buy"
	AllocationType string    `json:"allocation_type"`
	Amount         int       `json:"amount"`
	Order          int       `json:"order"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type RebalanceTransaction struct {
}
