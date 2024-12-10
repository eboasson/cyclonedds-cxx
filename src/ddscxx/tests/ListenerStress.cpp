// Copyright(c) 2006 to 2021 ZettaScale Technology and others
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Eclipse Distribution License
// v. 1.0 which is available at
// http://www.eclipse.org/org/documents/edl-v10.php.
//
// SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

#include <chrono>
#include <thread>

#include "dds/dds.hpp"
#include "dds/ddsrt/sync.h"
#include "dds/ddsrt/threads.h"

#include <gtest/gtest.h>
#include "HelloWorldData.hpp"

#define MAX_WRITERS 20
#define MAX_READERS 50

static ddsrt_mutex_t g_mutex;

class TestDomainParticipantListener : public virtual dds::domain::NoOpDomainParticipantListener
{
public:
    TestDomainParticipantListener() { }

protected:
    virtual void on_data_available(dds::sub::AnyDataReader& ) { }
};

class TestSubscriberListener : public virtual dds::sub::NoOpSubscriberListener
{
public:
    TestSubscriberListener() { }

protected:
    virtual void on_data_available(dds::sub::AnyDataReader& ) { }
};

class TestDataReaderListener : public virtual dds::sub::NoOpDataReaderListener<HelloWorldData::Msg>
{
public:
    TestDataReaderListener() { }

protected:
    virtual void on_data_available(dds::sub::DataReader<HelloWorldData::Msg>& ) { }
};


static bool stop_writer_thread = false;
static std::vector< dds::pub::DataWriter<HelloWorldData::Msg> > writers;


static uint32_t writer_thread(void *arg)
{
    uintptr_t i = reinterpret_cast<uintptr_t>(arg);

    ddsrt_mutex_lock(&g_mutex);
    dds::pub::DataWriter<HelloWorldData::Msg> writer = writers[i];
    ddsrt_mutex_unlock(&g_mutex);

    HelloWorldData::Msg testData(1, "test");

    while (!stop_writer_thread) {
        try {
            writer << testData;
        } catch (const dds::core::Exception& e) {
            std::cout << "Writer [" << i << "] write fails: " << e.what() << std::endl;
            stop_writer_thread = true;
        } catch (...) {
            std::cout << "Writer [" << i << "] write fails" << std::endl;
            stop_writer_thread = true;
        }
    }

    return 0;
}


/**
 * Fixture for listener stress tests
 */
class listener_stress : public ::testing::Test
{
public:
    dds::domain::DomainParticipant dp;
    dds::topic::Topic<HelloWorldData::Msg> topic;
    dds::pub::Publisher pub;
    dds::sub::Subscriber sub;

    TestDomainParticipantListener participantListener;
    TestSubscriberListener subListener;

    ddsrt_thread_t threadId[MAX_WRITERS];
    ddsrt_threadattr_t threadAttr;
    dds_duration_t delay = 200000000;  // 200ms

    listener_stress() :
        dp(dds::core::null),
        topic(dds::core::null),
        pub(dds::core::null),
        sub(dds::core::null)
    {
        ddsrt_threadattr_init(&threadAttr);
        memset(threadId, 0, sizeof(threadId));
    }

    void SetUp() {
        ddsrt_mutex_init(&g_mutex);

        // Create participant
        dp = dds::domain::DomainParticipant(org::eclipse::cyclonedds::domain::default_id());
        ASSERT_NE(dp, dds::core::null);

        // Create topic
        topic = dds::topic::Topic<HelloWorldData::Msg>(dp, "topic");
        ASSERT_NE(topic, dds::core::null);

        // Create publisher
        pub = dds::pub::Publisher(dp);
        ASSERT_NE(pub, dds::core::null);

        // Create subscriber
        sub = dds::sub::Subscriber(dp);
        ASSERT_NE(sub, dds::core::null);

        // Create writer threads
        for (uintptr_t i = 0; i < MAX_WRITERS; i++) {
            ddsrt_mutex_lock(&g_mutex);
            writers.push_back(dds::pub::DataWriter<HelloWorldData::Msg>(pub, topic));
            (void)ddsrt_thread_create(
                &threadId[i], "writer_thread", &threadAttr, writer_thread, reinterpret_cast<void*>(i));
            ddsrt_mutex_unlock(&g_mutex);
        }

        dds_sleepfor(delay);
    }

    void create_readers(dds::sub::Subscriber& sub_, dds::topic::Topic<HelloWorldData::Msg>& topic_, bool withListener)
    {
        uint32_t i;
        TestDataReaderListener readerListener;
        dds::sub::DataReader<HelloWorldData::Msg> reader = dds::core::null;

        // Create readers
        for (i = 0; i < MAX_READERS; i++) {
            try {
                if (withListener) {
                    reader = dds::sub::DataReader<HelloWorldData::Msg>(sub_, topic_, sub.default_datareader_qos(),
                            &readerListener, dds::core::status::StatusMask::data_available());
                } else {
                    reader = dds::sub::DataReader<HelloWorldData::Msg>(sub_, topic_);
                }
                if (reader != dds::core::null) {
                    dds_sleepfor(delay);
                } else {
                    FAIL() << "reader = dds::core::null";
                }
            } catch (const dds::core::Exception& e) {
                FAIL() << "Exception: " << e.what();
            } catch (...) {
                FAIL() << "Unknown exception";
            }
        }
    }

    void TearDown() {
        // Clean up writers
        uint32_t i;
        stop_writer_thread = true;
        for (i = 0; i < MAX_WRITERS; i++) {
            (void)ddsrt_thread_join(threadId[i], NULL);
            ddsrt_mutex_lock(&g_mutex);
            writers[i] = dds::core::null;
            ddsrt_mutex_unlock(&g_mutex);
        }

        pub.close();
        sub.close();
        sub = dds::core::null;
        pub = dds::core::null;

        dp.close();
        dp = dds::core::null;

        ddsrt_mutex_destroy(&g_mutex);
    }
};

// TODO: disabled because test fails for current API implementation, as there is no
// ref to the entity on which the callback is called during callback execution, and
// therefore the check in EntityDelegate destructor fails.
TEST_F(listener_stress, DISABLED_data_available_reader)
{
    create_readers(sub, topic, true);
}

TEST_F(listener_stress, DISABLED_data_available_subscriber)
{
    // Add listener on subscriber
    sub.listener(&subListener, dds::core::status::StatusMask::data_available());

    // Create readers
    create_readers(sub, topic, false);
}

TEST_F(listener_stress, DISABLED_data_available_participant)
{
    // Add listener on participant
    dp.listener(&participantListener, dds::core::status::StatusMask::data_available());

    // Create readers
    create_readers(sub, topic, false);
}

/////////////////////////////////
/////////////////////////////////

#include <chrono>
#include <thread>
#include <future>
#include <print>

using namespace std::chrono_literals;

//#define DEBUG std::println
//#define INFO std::println
#define DEBUG(...) do { } while (0)
#define INFO(...) do { } while (0)
#define WARN std::println
#define ERROR std::println

// Code for DataWriterListener and DataReaderListener
template<typename T>
class DataWriterListener : public dds::pub::NoOpDataWriterListener<T>
{
public:
  DataWriterListener(std::string topicName)
  : topicName_(std::move(topicName))
  {
    DEBUG("creating DataWriterListener for topic '{}'...", topicName_);
  }

  ~DataWriterListener() override { DEBUG("destroying DataWriterListener for topic '{}'...", topicName_); }

  void on_publication_matched([[maybe_unused]] dds::pub::DataWriter<T>& writer,
                              const dds::core::status::PublicationMatchedStatus& status) override
  {
    const bool isNegative = status.total_count_change() < 0 || status.current_count_change() < 0;
    
    if (isNegative) {
      INFO("DataWriter '{}' unmatched (lost) publications.  Current count: {}, change: {}",
                 topicName_,
                 status.current_count(),
                 status.current_count_change());
    } else {
      INFO("DataReader '{}' matched (gained) publications.  Current count: {}, change: {}",
                 topicName_,
                 status.current_count(),
                 status.current_count_change());
    }
  }

  void on_offered_incompatible_qos([[maybe_unused]] dds::pub::DataWriter<T>& writer,
                                   const dds::core::status::OfferedIncompatibleQosStatus& status) override
  {
    ERROR(
                "DataWriter '{}' offered incompatible QoS settings!  Total count: {}, change: {}, last_policy_id: {}",
                topicName_,
                status.total_count(),
                status.total_count_change(),
                status.last_policy_id());
    
    dds::core::policy::QosPolicyCountSeq qos_seq = status.policies();
    if (!qos_seq.empty()) {
      ERROR("DataWriter '{}' incompatible policy count: {}, policy_id of first incompatible QoS policy: {}",
                  topicName_,
                  qos_seq.size(),
                  qos_seq[0].policy_id());
    }
  }

  void on_offered_deadline_missed([[maybe_unused]] dds::pub::DataWriter<T>& writer,
                                  const dds::core::status::OfferedDeadlineMissedStatus& status) override
  {
    WARN("DataWriter '{}' missed offered deadline!  Total count: {}, change: {}",
               topicName_,
               status.total_count(),
               status.total_count_change());
  }

  void on_liveliness_lost([[maybe_unused]] dds::pub::DataWriter<T>& writer,
                          const dds::core::status::LivelinessLostStatus& status) override
  {
    const bool isNegative = status.total_count_change() < 0;

    if (isNegative) {
      INFO("DataWriter '{}' lost liveliness of publications.  Count: {}, change: {}",
                 topicName_,
                 status.total_count(),
                 status.total_count_change());
    } else {
      DEBUG("DataWriter '{}' gained liveliness of publications.  Count: {}, change: {}",
                  topicName_,
                  status.total_count(),
                  status.total_count_change());
    }
  }

private:
  const std::string topicName_{};
};

template<typename T>
class DataReaderListener : public dds::sub::NoOpDataReaderListener<T>
{
public:
  DataReaderListener(std::string topicName)
  : topicName_(std::move(topicName))
  {
    DEBUG("creating DataReaderListener for topic '{}'...", topicName_);
  }

  ~DataReaderListener() override { DEBUG("destroying DataReaderListener for topic '{}'...", topicName_); }

  void on_sample_lost([[maybe_unused]] dds::sub::DataReader<T>& reader,
                      const dds::core::status::SampleLostStatus& status) override
  {
    WARN("DataReader '{}' lost samples! Total count: {}, change: {}",
               topicName_,
               status.total_count(),
               status.total_count_change());
  }

  void on_subscription_matched([[maybe_unused]] dds::sub::DataReader<T>& reader,
                               const dds::core::status::SubscriptionMatchedStatus& status) override
  {
    const bool isNegative = status.total_count_change() < 0 || status.current_count_change() < 0;
    
    if (isNegative) {
      INFO("DataReader '{}' unmatched (lost) subscriptions.  Current count: {}, change: {}",
                 topicName_,
                 status.current_count(),
                 status.current_count_change());
    } else {
      INFO("DataReader '{}' matched (gained) subscriptions.  Current count: {}, change: {}",
                 topicName_,
                 status.current_count(),
                 status.current_count_change());
    }
  }

  void on_sample_rejected([[maybe_unused]] dds::sub::DataReader<T>& reader,
                          const dds::core::status::SampleRejectedStatus& status) override
  {
    WARN("DataReader '{}' had samples rejected!  Total count: {}, change: {}",
               topicName_,
               status.total_count(),
               status.total_count_change());
  }

  void on_requested_incompatible_qos([[maybe_unused]] dds::sub::DataReader<T>& reader,
                                     const dds::core::status::RequestedIncompatibleQosStatus& status) override
  {
    ERROR(
                "DataReader '{}' requested incompatible QoS settings!  Total count: {}, change: {}, last_policy_id: {}",
                topicName_,
                status.total_count(),
                status.total_count_change(),
                status.last_policy_id());
    
    dds::core::policy::QosPolicyCountSeq qos_seq = status.policies();
    if (!qos_seq.empty()) {
      ERROR("DataReader '{}' incompatible policy count: {}, policy_id of first incompatible QoS policy: {}",
                  topicName_,
                  qos_seq.size(),
                  qos_seq[0].policy_id());
    }
  }

  void on_requested_deadline_missed([[maybe_unused]] dds::sub::DataReader<T>& reader,
                                    const dds::core::status::RequestedDeadlineMissedStatus& status) override
  {
    WARN("DataReader '{}' missed requested deadline!  Total count: {}, change: {}",
               topicName_,
               status.total_count(),
               status.total_count_change());
  }

  void on_liveliness_changed([[maybe_unused]] dds::sub::DataReader<T>& reader,
                             const dds::core::status::LivelinessChangedStatus& status) override
  {
    const bool isNegative = status.alive_count_change() < 0 || status.not_alive_count_change() > 0;
  
    if (isNegative) {
      INFO("DataReader '{}' lost liveliness of some subscriptions.  Alive count: {}, change: {}; Not alive "
                 "count: {}, change: {}",
                 topicName_,
                 status.alive_count(),
                 status.alive_count_change(),
                 status.not_alive_count(),
                 status.not_alive_count_change());
    } else {
      DEBUG("DataReader '{}' gained liveliness of some subscriptions.  Alive count: {}, change: {}; Not "
                  "alive count: {}, change: {}",
                  topicName_,
                  status.alive_count(),
                  status.alive_count_change(),
                  status.not_alive_count(),
                  status.not_alive_count_change());
    }
  }

  void on_data_available([[maybe_unused]] dds::sub::DataReader<T>& reader) override {}

private:
  const std::string topicName_{};
};

TEST(DDSTopicSanityTest, writeWithReadersMatched_multiNativeReaderSameThread)
{
  auto topicName = "ReadWriteTest";
  using test_msg = HelloWorldData::Msg;
  
  dds::domain::DomainParticipant participant{0};
  auto pub = dds::pub::Publisher{participant};
  auto sub = dds::sub::Subscriber{participant};
  auto topic = dds::topic::Topic<test_msg>{participant, topicName};
  
  dds::pub::qos::DataWriterQos writerQos{};
  dds::sub::qos::DataReaderQos readerQos{};
  dds::core::status::StatusMask maskAll{dds::core::status::StatusMask::all()};
  dds::core::status::StatusMask readerMask{dds::core::status::StatusMask::none()};
  readerMask |= dds::core::status::StatusMask::requested_deadline_missed();
  readerMask |= dds::core::status::StatusMask::requested_incompatible_qos();
  readerMask |= dds::core::status::StatusMask::sample_lost();
  readerMask |= dds::core::status::StatusMask::sample_rejected();
  readerMask |= dds::core::status::StatusMask::liveliness_changed();
  readerMask |= dds::core::status::StatusMask::subscription_matched();
  
  // Writer
  DataWriterListener<test_msg> writerListener(topicName);
  std::optional<dds::pub::DataWriter<test_msg>> writer;
  EXPECT_NO_THROW(writer.emplace(pub, topic, writerQos, &writerListener, maskAll));
  
  std::promise<void> p1;
  auto f1 = p1.get_future();
  std::thread t([&writer, &p1] {
    auto timeout{1ms}; // set this timer sleep just to mimic the real test case
    std::this_thread::sleep_for(timeout);
    p1.set_value();
    writer->write(test_msg{});
  });
  
  f1.wait();
  //std::this_thread::sleep_for(200ms);
  
  // Readers
  std::vector<dds::sub::DataReader<test_msg>> readers{};
  std::vector<DataReaderListener<test_msg>> listeners{};
  for (size_t i = 0; i < 100; i++) {
    listeners.push_back(DataReaderListener<test_msg>(topicName));
  }
  for (size_t i = 0; i < 100; i++) {
    readers.emplace_back(dds::sub::DataReader<test_msg>{sub, topic, readerQos, &listeners[i], maskAll});
  }
  t.join();
  
  // destructor order can be listeners-then-readers
  // if so, listeners can still be invoked even though the listener object have been freed already
  // can't copy the object (because it contains state meaningful to the application)
  // so have to destroy readers explicitly
  //
  // Then it turns out a on_subscription_matched can bubble up from the network stack (if multiple
  // copies are run in parallel), increment the refcount of the underlying shared_ptr in the wrapper
  // used to convert the C listener call into a C++ listener call just before the main thread runs
  // the shared_ptr's destructor, leaving the listener with the sole remaining (strong) reference.
  //
  // When that happens, the listener will invoke the DataReader destructor, which will then hang on
  // stopping the listeners because it waits for any currently executing listeners to complete ...
  //
  // Removing the listener before the destructor runs avoids that problem.
  for (auto& rd : readers) {
    rd.listener (nullptr, maskAll);
  }
  readers.clear();
  writer->listener (nullptr, maskAll);
}

class CycloneMultiPubSub : public ::testing::Test
{
  using test_msg = HelloWorldData::Msg;
  
protected:
  void SetUp() override
  {
    participant = dds::domain::DomainParticipant{0};
    pub = dds::pub::Publisher{participant};
    sub = dds::sub::Subscriber{participant};
    topic = dds::topic::Topic<test_msg>{participant, topicName};
  }
  
  const std::string topicName{"test_multi_pubsub_topic"};
  const uint32_t multiEntityCnt{100};
  
  dds::domain::DomainParticipant participant{dds::core::null};
  dds::topic::Topic<test_msg> topic{dds::core::null};
  dds::pub::Publisher pub{dds::core::null};
  dds::sub::Subscriber sub{dds::core::null};
  dds::pub::qos::DataWriterQos writerQos{};
  dds::sub::qos::DataReaderQos readerQos{};
  dds::core::status::StatusMask maskAll{dds::core::status::StatusMask::all()};
  dds::core::status::StatusMask maskNone{dds::core::status::StatusMask::none()};
};

TEST_F(CycloneMultiPubSub, MultiWriterSingleReader)
{
  using test_msg = HelloWorldData::Msg;
  
  // declare writer
  DataWriterListener<test_msg> writerListener(topicName);
  std::optional<dds::pub::DataWriter<test_msg>> writer;
  EXPECT_NO_THROW(writer.emplace(pub, topic, writerQos, &writerListener, maskAll));
  
  // declare readers in multi threads
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < multiEntityCnt; ++i) {
    threads.emplace_back(std::thread([this] {
      DataReaderListener<test_msg> readerListener(topicName);
      auto reader = dds::sub::DataReader<test_msg>{sub, topic, readerQos, &readerListener, maskAll};
      reader->listener(nullptr, maskNone);
    }));
  }
  
  writer.value()->listener(nullptr, maskNone);
  writer.reset();
  
  for (auto&& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }
}

TEST_F(CycloneMultiPubSub, MultiReaderSingleWriter)
{
  using test_msg = HelloWorldData::Msg;
  
  // declare reader
  DataReaderListener<test_msg> readerListener(topicName);
  std::optional<dds::sub::DataReader<test_msg>> reader;
  EXPECT_NO_THROW(reader.emplace(sub, topic, readerQos, &readerListener, maskAll));
  
  // declare writers in multi threads
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < multiEntityCnt; ++i) {
    threads.emplace_back(std::thread([this] {
      DataWriterListener<test_msg> writerListener(topicName);
      auto writer = dds::pub::DataWriter<test_msg>{pub, topic, writerQos, &writerListener, maskAll};
      writer->listener(nullptr, maskNone);
    }));
  }
  
  reader.value()->listener(nullptr, maskNone);
  reader.reset();
  
  for (auto&& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }
}
