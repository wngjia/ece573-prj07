package main

import (
	"fmt"
	"log"

	"ece573-prj07/shared/marketplace"
	"ece573-prj07/shared/queue"

	"github.com/IBM/sarama"
)

// consumerHandler implements sarama.ConsumerGroupHandler. Sarama can call
// ConsumeClaim concurrently for different assigned partitions, so the handler
// shares only the Postgres store across claims.
type consumerHandler struct {
	store *marketplace.Store
}

// Setup runs at the start of each group assignment. Postgres, rather than
// Kafka's committed group offset, determines where each claim resumes.
func (h *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			next, err := h.store.NextOffset(session.Context(), topic, partition)
			if err != nil {
				if session.Context().Err() != nil {
					return fmt.Errorf("read %s[%d] checkpoint: %w", topic, partition, err)
				}
				log.Fatalf("FATAL read Postgres checkpoint %s[%d]: %v", topic, partition, err)
			}
			// Sarama's ResetOffset only moves backward. Mark then Reset forces
			// the initial claim position to the Postgres checkpoint in either
			// direction; Postgres remains the authoritative recovery position.
			session.MarkOffset(topic, partition, next, "")
			session.ResetOffset(topic, partition, next, "")
			log.Printf("INFO event=partition_assigned topic=%s partition=%d next_offset=%d", topic, partition, next)
		}
	}
	return nil
}

// Cleanup runs after the claim loops stop and records which partitions leave
// this worker. Their durable progress is already in Postgres.
func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			log.Printf("INFO event=partition_revoked topic=%s partition=%d", topic, partition)
		}
	}
	return nil
}

// ConsumeClaim processes one assigned partition in offset order until Sarama
// closes the claim or cancels the group session.
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("INFO event=claim_started topic=%s partition=%d initial_offset=%d",
		claim.Topic(), claim.Partition(), claim.InitialOffset())
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			command, err := queue.DecodeCommand(message)
			if err != nil {
				return err
			}
			log.Printf("INFO event=command_processing topic=%s partition=%d offset=%d job_id=%s kind=%s",
				message.Topic, message.Partition, message.Offset, command.JobID, command.Kind)
			result, err := h.store.Process(session.Context(), command, message.Topic, message.Partition, message.Offset)
			if err != nil {
				if session.Context().Err() != nil {
					return nil
				}
				log.Fatalf("FATAL process job=%s topic=%s partition=%d offset=%d: %v",
					command.JobID, message.Topic, message.Partition, message.Offset, err)
			}
			log.Printf("INFO event=checkpoint_committed topic=%s partition=%d next_offset=%d job_id=%s kind=%s skipped=%t rejected=%t conflict=%t",
				message.Topic, message.Partition, message.Offset+1, command.JobID, command.Kind,
				result.Skipped, result.Rejected, result.Conflict)
		}
	}
}
