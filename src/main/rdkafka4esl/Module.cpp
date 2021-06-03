#include <rdkafka4esl/Module.h>
#include <rdkafka4esl/com/basic/broker/Client.h>

#include <esl/com/basic/broker/Interface.h>
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

	addInterface(std::unique_ptr<const esl::module::Interface>(new esl::com::basic::broker::Interface(
			getId(), com::basic::broker::Client::getImplementation(), &com::basic::broker::Client::create)));
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
