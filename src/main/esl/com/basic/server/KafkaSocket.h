#ifndef ESL_COM_BASIC_SERVER_KAFKASOCKET_H_
#define ESL_COM_BASIC_SERVER_KAFKASOCKET_H_

#include <esl/com/basic/server/RequestHandler.h>
#include <esl/com/basic/server/Socket.h>
#include <esl/object/Context.h>
#include <esl/object/InitializeContext.h>
#include <esl/object/Object.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace esl {
inline namespace v1_6 {
namespace com {
namespace basic {
namespace server {

class KafkaSocket : public Socket, public object::InitializeContext {
public:
	struct Settings {
		Settings(const std::vector<std::pair<std::string, std::string>>& settings);
		std::string brokerId;
		std::uint16_t maxThreads = 0;
		bool stopIfEmpty = false;
		int pollTimeoutMs = 500;
		std::vector<std::pair<std::string, std::string>> kafkaSettings;
	};

	KafkaSocket(const Settings& settings);

	static std::unique_ptr<Socket> create(const std::vector<std::pair<std::string, std::string>>& settings);

	/* this method is blocking. */
	void listen(const RequestHandler& requestHandler) override;

	/* this method is non-blocking. A separate thread will be opened to listen */
	void listen(const RequestHandler& requestHandler, std::function<void()> onReleasedHandler) override;

	void release() override;

	void initializeContext(object::Context& context) override;

private:
	KafkaSocket(std::tuple<std::unique_ptr<Object>, Socket&, object::InitializeContext&> object);

	std::tuple<std::unique_ptr<Object>, Socket&, object::InitializeContext&> object;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* inline namespace v1_6 */
} /* namespace esl */

#endif /* ESL_COM_BASIC_SERVER_KAFKASOCKET_H_ */
