package coordinator

import "sort"

type TopicMetadataProvider interface {
	TopicPartitions(topic string) []int32
}

func BuildRoundRobinAssignment(
	memberIDs []string,
	subscriptions map[string][]string, // memberID -> topics
	meta TopicMetadataProvider,
) map[string][]TopicPartition {
	sort.Strings(memberIDs)

	all := make([]TopicPartition, 0)
	for _, topics := range subscriptions {
		for _, topic := range topics {
			for _, p := range meta.TopicPartitions(topic) {
				all = append(all, TopicPartition{Topic: topic, Partition: p})
			}
		}
		break // MVP: assume all members subscribe same topics
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Topic == all[j].Topic {
			return all[i].Partition < all[j].Partition
		}
		return all[i].Topic < all[j].Topic
	})

	out := make(map[string][]TopicPartition, len(memberIDs))
	if len(memberIDs) == 0 {
		return out
	}
	for i, tp := range all {
		memberID := memberIDs[i%len(memberIDs)]
		out[memberID] = append(out[memberID], tp)
	}
	return out
}
