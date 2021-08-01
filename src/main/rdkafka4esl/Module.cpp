#include <rdkafka4esl/Module.h>
#include <rdkafka4esl/com/basic/broker/Client.h>

#include <esl/com/http/client/Interface.h>
#include <esl/Module.h>

namespace rdkafka4esl {

void Module::install(esl::module::Module& module) {
	esl::setModule(module);

	module.addInterface(esl::com::basic::broker::Interface::createInterface(
			com::basic::broker::Client::getImplementation(),
			&com::basic::broker::Client::create));
}

} /* namespace rdkafka4esl */
