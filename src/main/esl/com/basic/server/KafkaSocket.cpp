#include <esl/com/basic/server/KafkaSocket.h>
#include <esl/system/Stacktrace.h>
#include <esl/utility/String.h>

#include <stdexcept>

#include <rdkafka4esl/com/basic/server/Socket.h>

namespace esl {
inline namespace v1_6 {
namespace com {
namespace basic {
namespace server {

namespace {
std::tuple<std::unique_ptr<object::Object>, Socket&, object::InitializeContext&> createKafkaSocket(const KafkaSocket::Settings& settings) {
	std::unique_ptr<rdkafka4esl::com::basic::server::Socket> rdKafkaSocket(new rdkafka4esl::com::basic::server::Socket(settings));

	Socket& socket = *rdKafkaSocket;
	object::InitializeContext& initContext = *rdKafkaSocket;
	std::unique_ptr<object::Object> object(rdKafkaSocket.release());

	return std::tuple<std::unique_ptr<object::Object>, Socket&, object::InitializeContext&>(std::move(object), socket, initContext);
}
}

KafkaSocket::Settings::Settings(const std::vector<std::pair<std::string, std::string>>& settings) {
	std::string groupId;
	bool hasMaxThreads = false;
	bool hasStopOnEmpty = false;
	bool hasPollTimeoutMs = false;

	for(auto& setting : settings) {
		if(setting.first == "broker-id") {
			if(!brokerId.empty()) {
				throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
			}

			brokerId = setting.second;
			if(brokerId.empty()) {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"\" for key '" + setting.first + "'"));
			}
		}
		else if(setting.first == "threads") {
			if(hasMaxThreads) {
				throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
			}
			hasMaxThreads = true;

			int i = esl::utility::String::toNumber<int>(setting.second);
			if(i < 0) {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid negative value for \"" + setting.first + "\"=\"" + setting.second + "\""));
			}

			maxThreads = static_cast<std::uint16_t>(i);
		}
		else if(setting.first == "stop-on-empty") {
			if(hasStopOnEmpty) {
				throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
			}
			hasStopOnEmpty = true;

			std::string value = esl::utility::String::toLower(setting.second);
			if(value == "true") {
				stopIfEmpty = true;
			}
			else if(value == "false") {
				stopIfEmpty = false;
			}
			else {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"" + setting.second + "\" for key '" + setting.first + "'"));
			}
		}
		else if(setting.first == "poll-timeout-ms") {
			if(hasPollTimeoutMs) {
				throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
			}
			hasPollTimeoutMs = true;

			pollTimeoutMs = esl::utility::String::toNumber<decltype(pollTimeoutMs)>(setting.second);
			if(pollTimeoutMs < 1) {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"" + setting.second + "\" for key '" + setting.first + "-ms'"));
			}
		}
		else if(setting.first == "group-id") {
			if(!groupId.empty()) {
				throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
			}

			groupId = setting.second;
			if(groupId.empty()) {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"\" for key '" + setting.first + "'"));
			}
		}
		else if(setting.first.size() > 6 && setting.first.substr(0, 6) == "kafka.") {
			std::string kafkaKey = setting.first.substr(6);
			if(kafkaKey == "group.id") {
				if(!groupId.empty()) {
					throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute '" + setting.first + "'."));
				}

				groupId = setting.second;
				if(groupId.empty()) {
					throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"\" for key '" + setting.first + "'"));
				}
			}
			else {
				kafkaSettings.emplace_back(kafkaKey, setting.second);
			}
		}
		else {
			throw esl::system::Stacktrace::add(std::runtime_error("Unknown key '" + setting.first + "'"));
		}
	}

#if 1
	if(groupId.empty()) {
		if(brokerId.empty()) {
			throw esl::system::Stacktrace::add(std::runtime_error("Key 'broker-id' is missing"));
		}
		else {
			throw esl::system::Stacktrace::add(std::runtime_error("Key 'kafka.group.id' is missing."));
		}
	}
	kafkaSettings.emplace_back("group.id", groupId);
#else
	if(kafkaSettings.empty()) {
		if(brokerId.empty()) {
			throw esl::system::Stacktrace::add(std::runtime_error("Key 'broker-id' is missing"));
		}
	}
	else {
		if(groupId.empty()) {
			throw esl::system::Stacktrace::add(std::runtime_error("Key 'kafka.group.id' is missing."));
		}
	}
#endif
}

KafkaSocket::KafkaSocket(const Settings& settings)
: KafkaSocket(createKafkaSocket(settings))
{ }

KafkaSocket::KafkaSocket(std::tuple<std::unique_ptr<Object>, Socket&, object::InitializeContext&> aObject)
: object(std::move(aObject))
{ }

std::unique_ptr<Socket> KafkaSocket::create(const std::vector<std::pair<std::string, std::string>>& settings) {
	return std::unique_ptr<Socket>(new KafkaSocket(Settings(settings)));
}

/* this method is blocking. */
void KafkaSocket::listen(const RequestHandler& requestHandler) {
	std::get<1>(object).listen(requestHandler);
}

/* this method is non-blocking. A separate thread will be opened to listen */
void KafkaSocket::listen(const RequestHandler& requestHandler, std::function<void()> onReleasedHandler) {
	std::get<1>(object).listen(requestHandler, onReleasedHandler);
}

void KafkaSocket::release() {
	std::get<1>(object).release();
}

void KafkaSocket::initializeContext(object::Context& context) {
	std::get<2>(object).initializeContext(context);
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* inline namespace v1_6 */
} /* namespace esl */
