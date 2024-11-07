package main

type Handler struct {
	db *Db
}

func NewHandler(db *Db) *Handler {
	return &Handler{
		db: db,
	}
}

func (h *Handler) ProcessMessage(msg Message) {
	h.db.SomeDatabaseQuery(msg.Body)
}
