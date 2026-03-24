package amqpcore

import "erionn-mq/internal/store"

// Message is an alias so broker callers only need to import amqpcore.
// The actual storage type lives in the store package to avoid circular deps.
type Message = store.Message
