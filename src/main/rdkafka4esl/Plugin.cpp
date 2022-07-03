#include <rdkafka4esl/com/basic/broker/Client.h>
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/Plugin.h>

#include <esl/com/basic/client/ConnectionFactory.h>
#include <esl/com/basic/server/Socket.h>
#include <esl/object/Object.h>

namespace rdkafka4esl {

void Plugin::install(esl::plugin::Registry& registry, const char* data) {
	esl::plugin::Registry::set(registry);

	registry.addPlugin<esl::object::Object>(
			"rdkafka4esl/com/basic/broker/Client",
			&com::basic::broker::Client::create);

	registry.addPlugin<esl::com::basic::client::ConnectionFactory>(
			"rdkafka4esl/com/basic/client/ConnectionFactory",
			&com::basic::client::ConnectionFactory::create);

	registry.addPlugin<esl::com::basic::server::Socket>(
			"rdkafka4esl/com/basic/server/Socket",
			&com::basic::server::Socket::create);
}

} /* namespace rdkafka4esl */
