#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_

#include <esl/com/basic/server/Interface.h>
#include <esl/com/basic/server/requesthandler/Interface.h>
#include <esl/object/InitializeContext.h>
#include <esl/object/Interface.h>
#include <esl/module/Interface.h>

#include <librdkafka/rdkafka.h>

#include <memory>
#include <functional>
#include <cstdint>
#include <mutex>
#include <condition_variable>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {
class Client;
}
namespace server {

class Socket : public virtual esl::com::basic::server::Interface::Socket, public esl::object::InitializeContext {
public:
	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	static std::unique_ptr<esl::com::basic::server::Interface::Socket> create(const esl::module::Interface::Settings& settings);

	Socket(const esl::module::Interface::Settings& settings);
	~Socket();

	void initializeContext(esl::object::Interface::ObjectContext& objectContext) override;

	void listen(const esl::com::basic::server::requesthandler::Interface::RequestHandler& requestHandler, std::function<void()> onReleasedHandler) override;
	void release() override;
	bool wait(std::uint32_t ms) override;

private:
	std::string brokerId;
	std::uint16_t maxThreads = 0;
	bool stopIfEmpty = false;
	int pollTimeoutMs = 500;

	broker::Client* client = nullptr;
	esl::module::Interface::Settings kafkaSettings;

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

	void listen(const esl::com::basic::server::requesthandler::Interface::RequestHandler& requestHandler);
	void accept(rd_kafka_message_t& rdkMessage, const esl::com::basic::server::requesthandler::Interface::RequestHandler& requestHandler);
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_ */
