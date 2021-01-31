#include <rdkafka4esl/Module.h>
#include <rdkafka4esl/messaging/Client.h>

#include <esl/messaging/Interface.h>
#include <esl/module/Interface.h>
#include <esl/Stacktrace.h>

#include <stdexcept>
#include <memory>
#include <new>         // placement new
#include <type_traits> // aligned_storage

namespace rdkafka4esl {

namespace {

class Module : public esl::module::Module {
public:
	Module();
};

typename std::aligned_storage<sizeof(Module), alignof(Module)>::type moduleBuffer; // memory for the object;
Module* modulePtr = nullptr;

Module::Module()
: esl::module::Module()
{
	esl::module::Module::initialize(*this);

	addInterface(std::unique_ptr<const esl::module::Interface>(new esl::messaging::Interface(
			getId(), messaging::Client::getImplementation(), &messaging::Client::create)));
}

} /* anonymous namespace */

esl::module::Module& getModule() {
	if(modulePtr == nullptr) {
		/* ***************** *
		 * initialize module *
		 * ***************** */

		modulePtr = reinterpret_cast<Module*> (&moduleBuffer);
		new (modulePtr) Module; // placement new
	}

	return *modulePtr;
}

} /* namespace rdkafka4esl */
