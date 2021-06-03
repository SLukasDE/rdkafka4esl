#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/com/basic/broker/Client.h>

#include <esl/Stacktrace.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

Socket::Socket(broker::Client& aClient)
: client(aClient)
{ }

void Socket::addObjectFactory(const std::string& id, ObjectFactory objectFactory) {
	if(client.consumerIsStateNotRunning() == false) {
		throw esl::addStacktrace(std::runtime_error("Calling Client::addObjectFactory not allowed, because Kafka-Client is already listening"));
	}

	objectFactories[id] = objectFactory;
}

Socket::ObjectFactory Socket::getObjectFactory(const std::string& id) const {
	auto iter = objectFactories.find(id);
	if(iter != std::end(objectFactories)) {
		return iter->second;
	}
	return nullptr;
}

void Socket::listen(const std::set<std::string>& notifications, esl::com::basic::server::requesthandler::Interface::CreateInput createInput) {
	client.socketListen(notifications, createInput);
}

void Socket::release() {
	client.socketRelease();
}

bool Socket::wait(std::uint32_t ms) {
	return client.socketWait(ms);
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
