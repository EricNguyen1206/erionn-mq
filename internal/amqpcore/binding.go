package amqpcore

import (
	"strings"
)

// Binding represents the link between an exchange and a queue.
type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         map[string]any
}

// matches reports whether this binding should route msg to its queue,
// given the exchange type of the owning exchange.
func (b *Binding) matches(exchangeType ExchangeType, routingKey string) bool {
	switch exchangeType {
	case ExchangeDirect:
		return b.RoutingKey == routingKey
	case ExchangeFanout:
		return true
	case ExchangeTopic:
		return matchTopic(b.RoutingKey, routingKey)
	default:
		return false
	}
}

// matchTopic implements AMQP topic routing key pattern matching.
//
// Pattern words are dot-separated. Special tokens:
//   - '*' matches exactly one word
//   - '#' matches zero or more words
func matchTopic(pattern, key string) bool {
	patternParts := strings.Split(pattern, ".")
	keyParts := strings.Split(key, ".")
	return matchParts(patternParts, keyParts)
}

func matchParts(pattern, key []string) bool {
	// Base cases
	if len(pattern) == 0 && len(key) == 0 {
		return true
	}
	if len(pattern) == 0 {
		return false
	}

	if pattern[0] == "#" {
		// '#' can consume zero words → try skipping it entirely,
		// or consume one word at a time.
		if matchParts(pattern[1:], key) { // zero words consumed
			return true
		}
		if len(key) == 0 {
			return false
		}
		return matchParts(pattern, key[1:]) // consume one key word and retry
	}

	if len(key) == 0 {
		return false
	}

	if pattern[0] == "*" || pattern[0] == key[0] {
		return matchParts(pattern[1:], key[1:])
	}

	return false
}
