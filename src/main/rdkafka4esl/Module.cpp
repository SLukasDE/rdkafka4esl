#include <rdkafka4esl/Module.h>
#include <rdkafka4esl/com/basic/broker/Client.h>
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/com/basic/server/Socket.h>

#include <esl/object/Interface.h>
#include <esl/com/basic/client/Interface.h>
#include <esl/com/basic/server/Interface.h>
#include <esl/Module.h>

namespace rdkafka4esl {

void Module::install(esl::module::Module& module) {
	esl::setModule(module);

	module.addInterface(esl::object::Interface::createInterface(
			com::basic::broker::Client::getImplementation(),
			&com::basic::broker::Client::create));

	module.addInterface(esl::com::basic::client::Interface::createInterface(
			com::basic::client::ConnectionFactory::getImplementation(),
			&com::basic::client::ConnectionFactory::create));

	module.addInterface(esl::com::basic::server::Interface::createInterface(
			com::basic::server::Socket::getImplementation(),
			&com::basic::server::Socket::create));
}

} /* namespace rdkafka4esl */
