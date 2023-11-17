#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_

#include <esl/com/basic/server/KafkaSocket.h>
#include <esl/com/basic/server/RequestHandler.h>
#include <esl/com/basic/server/Socket.h>
#include <esl/object/InitializeContext.h>
#include <esl/object/Context.h>

#include <librdkafka/rdkafka.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace rdkafka4esl {
namespace com {
namespace basic {

namespace broker {
class Client;
}

namespace server {

class Socket : public esl::com::basic::server::Socket, public esl::object::InitializeContext {
public:
	Socket(const esl::com::basic::server::KafkaSocket::Settings& settings);
	~Socket();

	void initializeContext(esl::object::Context& objectContext) override;

	void listen(const esl::com::basic::server::RequestHandler& requestHandler) override;
	void listen(const esl::com::basic::server::RequestHandler& requestHandler, std::function<void()> onReleasedHandler) override;
	void release() override;

	bool wait(std::uint32_t ms);

private:
	esl::com::basic::server::KafkaSocket::Settings settings;

	broker::Client* client = nullptr;

	std::function<void()> onReleasedHandler;

	/* ****************** *
	 * Consumer variables *
	 * ****************** */

	rd_kafka_t* rdkConsumerHandle = nullptr;
	rd_kafka_topic_partition_list_t* rdkTopicPartitionList = nullptr;

	std::mutex stateNotifyMutex;
	std::condition_variable stateNotifyCondVar;

	mutable std::mutex stateMutex;
	enum {
		stopped,
		started,
		stopping
	} state = stopped;
	std::uint16_t threadsRunning = 0;

	std::mutex threadsNotifyMutex;
	std::condition_variable threadsCondVar;

	void listen2(const esl::com::basic::server::RequestHandler& requestHandler);
	void accept(rd_kafka_message_t& rdkMessage, const esl::com::basic::server::RequestHandler& requestHandler);
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_ */
