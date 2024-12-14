package metrics

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	once  sync.Once
	meter metric.Meter

	successfulMessagesCounter    metric.Int64Counter
	serializationErrorsCounter   metric.Int64Counter
	deserializationErrorsCounter metric.Int64Counter
	unsuccessfulMessagesCounter  metric.Int64Counter
	successfulRetriesCounter     metric.Int64Counter
	unsuccessfulRetriesCounter   metric.Int64Counter
	successfulCommitsCounter     metric.Int64Counter
	failedCommitsCounter         metric.Int64Counter
	ignoredEventsCounter         metric.Int64Counter
	sentMessagesCounter          metric.Int64Counter
	receivedMessagesCounter      metric.Int64Counter
)

func InitMeter(name string) {
	once.Do(func() {
		var err error
		meter = otel.Meter(name)

		successfulMessagesCounter, err = meter.Int64Counter("successful_messages",
			metric.WithDescription("Number of successful messages processed"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		serializationErrorsCounter, err = meter.Int64Counter("serialization_errors",
			metric.WithDescription("Number of serialization errors"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		deserializationErrorsCounter, err = meter.Int64Counter("deserialization_errors",
			metric.WithDescription("Number of deserialization errors"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		unsuccessfulMessagesCounter, err = meter.Int64Counter("unsuccessful_messages",
			metric.WithDescription("Number of unsuccessful messages processed"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		successfulRetriesCounter, err = meter.Int64Counter("successful_retries",
			metric.WithDescription("Number of successful retries"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		unsuccessfulRetriesCounter, err = meter.Int64Counter("unsuccessful_retries",
			metric.WithDescription("Number of unsuccessful retries"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		successfulCommitsCounter, err = meter.Int64Counter("successful_commits",
			metric.WithDescription("Number of successful commits"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		failedCommitsCounter, err = meter.Int64Counter("failed_commits",
			metric.WithDescription("Number of failed commits"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		ignoredEventsCounter, err = meter.Int64Counter("ignored_events",
			metric.WithDescription("Number of ignored events"),
			metric.WithUnit("1"))

		if err != nil {
			panic(err)
		}

		sentMessagesCounter, err = meter.Int64Counter("sent_messages",
			metric.WithDescription("Number of messages sent by the producer"),
			metric.WithUnit("1"))
		if err != nil {
			panic(err)
		}

		receivedMessagesCounter, err = meter.Int64Counter("received_messages",
			metric.WithDescription("Number of messages received by the consumer"),
			metric.WithUnit("1"))
		if err != nil {
			panic(err)
		}
	})
}

func GetMeter() metric.Meter {
	return meter
}

func GetSuccessfulMessagesCounter() metric.Int64Counter {
	return successfulMessagesCounter
}

func GetSerializationErrorsCounter() metric.Int64Counter {
	return serializationErrorsCounter
}

func GetDeserializationErrorsCounter() metric.Int64Counter {
	return deserializationErrorsCounter
}

func GetUnsuccessfulMessagesCounter() metric.Int64Counter {
	return unsuccessfulMessagesCounter
}

func GetSuccessfulRetriesCounter() metric.Int64Counter {
	return successfulRetriesCounter
}

func GetUnsuccessfulRetriesCounter() metric.Int64Counter {
	return unsuccessfulRetriesCounter
}

func GetSuccessfulCommitsCounter() metric.Int64Counter {
	return successfulCommitsCounter
}

func GetFailedCommitsCounter() metric.Int64Counter {
	return failedCommitsCounter
}

func GetIgnoredEventsCounter() metric.Int64Counter {
	return ignoredEventsCounter
}

func GetSentMessagesCounter() metric.Int64Counter {
	return sentMessagesCounter
}

func GetReceivedMessagesCounter() metric.Int64Counter {
	return receivedMessagesCounter
}
