/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/FutureUtil.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/io/async/EventBase.h>

#include <folly/logging/xlog.h>

namespace {
using namespace moxygen;
constexpr std::chrono::seconds kSetupTimeout(5);

constexpr uint32_t IdMask = 0x1FFFFF;
uint32_t groupPriorityBits(GroupOrder groupOrder, uint64_t group) {
  // If the group order is oldest first, we want to give lower group
  // ids a higher precedence. Otherwise, if it is newest first, we want
  // to give higher group ids a higher precedence.
  uint32_t truncGroup = static_cast<uint32_t>(group) & IdMask;
  return groupOrder == GroupOrder::OldestFirst ? truncGroup
                                               : (IdMask - truncGroup);
}

uint32_t subgroupPriorityBits(uint32_t subgroupID) {
  return static_cast<uint32_t>(subgroupID) & IdMask;
}

/*
 * The spec mentions that scheduling should go as per
 * the following precedence list:
 * (1) Higher subscriber priority
 * (2) Higher publisher priority
 * (3) Group order, if the objects belong to different groups
 * (4) Lowest subgroup id
 *
 * This function takes in the relevant parameters and encodes them into a stream
 * priority so that we respect the aforementioned precedence order when we are
 * sending objects.
 */
uint64_t getStreamPriority(
    uint64_t groupID,
    uint64_t subgroupID,
    uint8_t subPri,
    uint8_t pubPri,
    GroupOrder pubGroupOrder) {
  // 6 reserved bits | 58 bit order
  // 6 reserved | 8 sub pri | 8 pub pri | 21 group order | 21 obj order
  uint32_t groupBits = groupPriorityBits(pubGroupOrder, groupID);
  uint32_t subgroupBits = subgroupPriorityBits(subgroupID);
  return (
      (uint64_t(subPri) << 50) | (uint64_t(pubPri) << 42) | (groupBits << 21) |
      subgroupBits);
}

// Helper classes for publishing

// StreamPublisherImpl is for publishing to a single stream, either a Subgroup
// or a Fetch response.  It's of course illegal to mix-and-match the APIs, but
// the object is only handed to the application as either a SubgroupConsumer
// or a FetchConsumer
class StreamPublisherImpl : public SubgroupConsumer,
                            public FetchConsumer,
                            public folly::CancellationCallback {
 public:
  StreamPublisherImpl() = delete;

  // Fetch constructor
  StreamPublisherImpl(
      MoQSession::PublisherImpl* publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle);

  // Subscribe constructor
  StreamPublisherImpl(
      MoQSession::PublisherImpl* publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      TrackAlias alias,
      uint64_t groupID,
      uint64_t subgroupID);

  // SubgroupConsumer overrides
  // Note where the interface uses finSubgroup, this class uses finStream,
  // since it is used for fetch and subgroups
  folly::Expected<folly::Unit, MoQPublishError>
  object(uint64_t objectID, Payload payload, bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objectID,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectId,
      uint64_t length,
      Payload initialPayload) override;
  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectId) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectId) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override;
  void reset(ResetStreamErrorCode error) override;

  // FetchConsumer overrides
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return object(objectID, std::move(payload), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return objectNotExists(objectID, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(0, ObjectStatus::GROUP_NOT_EXIST, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return beginObject(objectID, length, std::move(initialPayload));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(objectID, ObjectStatus::END_OF_GROUP, finFetch);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return endOfTrackAndGroup(objectID);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    if (!writeHandle_) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
    }
    return endOfSubgroup();
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitReadyToConsume() override;

  folly::Expected<folly::Unit, MoQPublishError>
  publishStatus(uint64_t objectID, ObjectStatus status, bool finStream);

 private:
  bool setGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    if (groupID < header_.group) {
      return false;
    } else if (groupID > header_.group) {
      // TODO(T211026595): reverse this check with group order
      // Fetch group advanced, reset expected object
      header_.id = std::numeric_limits<uint64_t>::max();
    }
    header_.group = groupID;
    header_.subgroup = subgroupID;
    return true;
  }

  folly::Expected<folly::Unit, MoQPublishError> validatePublish(
      uint64_t objectID);
  folly::Expected<ObjectPublishStatus, MoQPublishError>
  validateObjectPublishAndUpdateState(folly::IOBuf* payload, bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeCurrentObject(
      uint64_t objectID,
      uint64_t length,
      Payload payload,
      bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeToStream(bool finStream);

  void onStreamComplete();

  MoQSession::PublisherImpl* publisher_{nullptr};
  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  StreamType streamType_;
  ObjectHeader header_;
  folly::Optional<uint64_t> currentLengthRemaining_;
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
};

class TrackPublisherImpl : public MoQSession::PublisherImpl,
                           public TrackConsumer {
 public:
  TrackPublisherImpl() = delete;
  TrackPublisherImpl(
      MoQSession* session,
      SubscribeID subscribeID,
      TrackAlias trackAlias,
      Priority subPriority,
      GroupOrder groupOrder)
      : PublisherImpl(session, subscribeID, subPriority, groupOrder),
        trackAlias_(trackAlias) {}

  // PublisherImpl overrides
  void onStreamComplete(const ObjectHeader& finalHeader) override;

  void reset(ResetStreamErrorCode) override {
    // TBD: reset all subgroups_?  Currently called from cleanup()
  }

  // TrackConsumer overrides
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override;

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override;

 private:
  TrackAlias trackAlias_;
  folly::F14FastMap<
      std::pair<uint64_t, uint64_t>,
      std::shared_ptr<StreamPublisherImpl>>
      subgroups_;
};

class FetchPublisherImpl : public MoQSession::PublisherImpl {
 public:
  FetchPublisherImpl(
      MoQSession* session,
      SubscribeID subscribeID,
      Priority subPriority,
      GroupOrder groupOrder)
      : PublisherImpl(session, subscribeID, subPriority, groupOrder) {}

  folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError> beginFetch(
      GroupOrder groupOrder);

  void reset(ResetStreamErrorCode error) override {
    if (streamPublisher_) {
      streamPublisher_->reset(error);
    }
  }

  void onStreamComplete(const ObjectHeader&) override {
    streamPublisher_.reset();
    PublisherImpl::fetchComplete();
  }

 private:
  std::shared_ptr<StreamPublisherImpl> streamPublisher_;
};

// TrackPublisherImpl

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
TrackPublisherImpl::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority pubPriority) {
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after subscribeDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after subscribeDone"));
  }
  auto stream = wt->createUniStream();
  if (!stream) {
    // failed to create a stream
    // TODO: can it fail for non-stream credit reasons? Session closing should
    // be handled above.
    XLOG(ERR) << "Failed to create uni stream tp=" << this;
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::BLOCKED, "Failed to create uni stream."));
  }
  XLOG(DBG4) << "New stream created, id: " << stream.value()->getID()
             << " tp=" << this;
  stream.value()->setPriority(
      1,
      getStreamPriority(
          groupID, subgroupID, subPriority_, pubPriority, groupOrder_),
      false);
  auto subgroupPublisher = std::make_shared<StreamPublisherImpl>(
      this, *stream, trackAlias_, groupID, subgroupID);
  // TODO: these are currently unused, but the intent might be to reset
  // open subgroups automatically from some path?
  subgroups_[{groupID, subgroupID}] = subgroupPublisher;
  return subgroupPublisher;
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
TrackPublisherImpl::awaitStreamCredit() {
  auto wt = getWebTransport();
  if (!wt) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "awaitStreamCredit after subscribeDone"));
  }
  return wt->awaitUniStreamCredit();
}

void TrackPublisherImpl::onStreamComplete(const ObjectHeader& finalHeader) {
  subgroups_.erase({finalHeader.group, finalHeader.subgroup});
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::objectStream(
    const ObjectHeader& objHeader,
    Payload payload) {
  XCHECK(objHeader.status == ObjectStatus::NORMAL || !payload);
  auto subgroup =
      beginSubgroup(objHeader.group, objHeader.subgroup, objHeader.priority);
  if (subgroup.hasError()) {
    return folly::makeUnexpected(std::move(subgroup.error()));
  }
  switch (objHeader.status) {
    case ObjectStatus::NORMAL:
      return subgroup.value()->object(
          objHeader.id, std::move(payload), /*finSubgroup=*/true);
    case ObjectStatus::OBJECT_NOT_EXIST:
      return subgroup.value()->objectNotExists(
          objHeader.id, /*finSubgroup=*/true);
    case ObjectStatus::GROUP_NOT_EXIST: {
      auto& subgroupPublisherImpl =
          static_cast<StreamPublisherImpl&>(*subgroup.value());
      return subgroupPublisherImpl.publishStatus(
          objHeader.id, objHeader.status, /*finStream=*/true);
    }
    case ObjectStatus::END_OF_GROUP:
      return subgroup.value()->endOfGroup(objHeader.id);
    case ObjectStatus::END_OF_TRACK_AND_GROUP:
      return subgroup.value()->endOfTrackAndGroup(objHeader.id);
    case ObjectStatus::END_OF_SUBGROUP:
      return subgroup.value()->endOfSubgroup();
  }
  return folly::makeUnexpected(
      MoQPublishError(MoQPublishError::WRITE_ERROR, "unreachable"));
}

folly::Expected<folly::Unit, MoQPublishError>
TrackPublisherImpl::groupNotExists(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority) {
  return objectStream(
      {trackAlias_,
       groupID,
       subgroupID,
       0,
       priority,
       ObjectStatus::GROUP_NOT_EXIST,
       0},
      nullptr);
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::datagram(
    const ObjectHeader& header,
    Payload payload) {
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after subscribeDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after subscribeDone"));
  }
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  XCHECK(header.length);
  (void)writeObject(
      writeBuf,
      StreamType::OBJECT_DATAGRAM,
      ObjectHeader{
          trackAlias_,
          header.group,
          header.id,
          header.id,
          header.priority,
          header.status,
          *header.length},
      std::move(payload));
  // TODO: set priority when WT has an API for that
  auto res = wt->sendDatagram(writeBuf.move());
  if (res.hasError()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::WRITE_ERROR, "sendDatagram failed"));
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::subscribeDone(
    SubscribeDone subDone) {
  subDone.subscribeID = subscribeID_;
  return PublisherImpl::subscribeDone(std::move(subDone));
}

// FetchPublisherImpl

folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError>
FetchPublisherImpl::beginFetch(GroupOrder groupOrder) {
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after fetchCancel";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after fetchCancel"));
  }

  auto stream = wt->createUniStream();
  if (!stream) {
    // failed to create a stream
    XLOG(ERR) << "Failed to create uni stream tp=" << this;
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::BLOCKED, "Failed to create uni stream."));
  }
  XLOG(DBG4) << "New stream created, id: " << stream.value()->getID()
             << " tp=" << this;
  setGroupOrder(groupOrder);
  // Currently sets group=0 for FETCH priority bits
  stream.value()->setPriority(
      1, getStreamPriority(0, 0, subPriority_, 0, groupOrder_), false);
  streamPublisher_ = std::make_shared<StreamPublisherImpl>(this, *stream);
  return streamPublisher_;
}

// StreamPublisherImpl

StreamPublisherImpl::StreamPublisherImpl(
    MoQSession::PublisherImpl* publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle)
    : CancellationCallback(
          writeHandle->getCancelToken(),
          [this] {
            if (writeHandle_) {
              auto code = writeHandle_->stopSendingErrorCode();
              XLOG(DBG1) << "Peer requested write termination code="
                         << (code ? folly::to<std::string>(*code)
                                  : std::string("none"));
              reset(ResetStreamErrorCode::CANCELLED);
            }
          }),
      publisher_(publisher),
      writeHandle_(writeHandle),
      streamType_(StreamType::FETCH_HEADER),
      header_{
          publisher->subscribeID(),
          0,
          0,
          std::numeric_limits<uint64_t>::max(),
          0,
          ObjectStatus::NORMAL,
          folly::none} {
  (void)writeFetchHeader(writeBuf_, publisher->subscribeID());
}

StreamPublisherImpl::StreamPublisherImpl(
    MoQSession::PublisherImpl* publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    TrackAlias alias,
    uint64_t groupID,
    uint64_t subgroupID)
    : StreamPublisherImpl(publisher, writeHandle) {
  streamType_ = StreamType::STREAM_HEADER_SUBGROUP;
  header_.trackIdentifier = alias;
  setGroupAndSubgroup(groupID, subgroupID);
  writeBuf_.move(); // clear FETCH_HEADER
  (void)writeSubgroupHeader(writeBuf_, header_);
}

// Private methods

void StreamPublisherImpl::onStreamComplete() {
  XCHECK_EQ(writeHandle_, nullptr);
  publisher_->onStreamComplete(header_);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::validatePublish(uint64_t objectID) {
  if (currentLengthRemaining_) {
    XLOG(ERR) << "Still publishing previous object sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Previous object incomplete"));
  }
  if (header_.id != std::numeric_limits<uint64_t>::max() &&
      objectID <= header_.id) {
    XLOG(ERR) << "Object ID not advancing header_.id=" << header_.id
              << " objectID=" << objectID << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Object ID not advancing in subgroup"));
  }
  if (!writeHandle_) {
    XLOG(ERR) << "Write after subgroup complete sgp=" << this;
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Subgroup reset"));
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeCurrentObject(
    uint64_t objectID,
    uint64_t length,
    Payload payload,
    bool finStream) {
  header_.id = objectID;
  header_.length = length;
  (void)writeObject(writeBuf_, streamType_, header_, std::move(payload));
  return writeToStream(finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeToStream(bool finStream) {
  auto writeHandle = writeHandle_;
  if (finStream) {
    writeHandle_ = nullptr;
  }
  auto writeRes = writeHandle->writeStreamData(writeBuf_.move(), finStream);
  if (writeRes.hasValue()) {
    if (finStream) {
      onStreamComplete();
    }
    return folly::unit;
  }
  XLOG(ERR) << "write error=" << uint64_t(writeRes.error());
  reset(ResetStreamErrorCode::INTERNAL_ERROR);
  return folly::makeUnexpected(
      MoQPublishError(MoQPublishError::WRITE_ERROR, "write error"));
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
StreamPublisherImpl::validateObjectPublishAndUpdateState(
    folly::IOBuf* payload,
    bool finStream) {
  auto length = payload ? payload->computeChainDataLength() : 0;
  if (!currentLengthRemaining_) {
    XLOG(ERR) << "Not publishing object sgp=" << this;
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Not publishing object"));
  }
  if (length > *currentLengthRemaining_) {
    XLOG(ERR) << "Length=" << length
              << " exceeds remaining=" << *currentLengthRemaining_
              << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Length exceeds remaining in object"));
  }
  *currentLengthRemaining_ -= length;
  if (*currentLengthRemaining_ == 0) {
    currentLengthRemaining_.reset();
    return ObjectPublishStatus::DONE;
  } else if (finStream) {
    XLOG(ERR) << "finStream with length remaining=" << *currentLengthRemaining_
              << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "finStream with open object"));
  }
  return ObjectPublishStatus::IN_PROGRESS;
}

// Interface Methods

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::object(
    uint64_t objectID,
    Payload payload,
    bool finStream) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  auto length = payload ? payload->computeChainDataLength() : 0;
  return writeCurrentObject(objectID, length, std::move(payload), finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::objectNotExists(uint64_t objectID, bool finStream) {
  return publishStatus(objectID, ObjectStatus::OBJECT_NOT_EXIST, finStream);
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::beginObject(
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  currentLengthRemaining_ = length;
  auto validateObjectPublishRes = validateObjectPublishAndUpdateState(
      initialPayload.get(),
      /*finStream=*/false);
  if (!validateObjectPublishRes) {
    return folly::makeUnexpected(validateObjectPublishRes.error());
  }
  return writeCurrentObject(
      objectID, length, std::move(initialPayload), /*finStream=*/false);
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
StreamPublisherImpl::objectPayload(Payload payload, bool finStream) {
  auto validateObjectPublishRes =
      validateObjectPublishAndUpdateState(payload.get(), finStream);
  if (!validateObjectPublishRes) {
    return validateObjectPublishRes;
  }
  writeBuf_.append(std::move(payload));
  auto writeRes = writeToStream(finStream);
  if (writeRes.hasValue()) {
    return validateObjectPublishRes.value();
  } else {
    return folly::makeUnexpected(writeRes.error());
  }
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::endOfGroup(
    uint64_t endOfGroupObjectId) {
  return publishStatus(
      endOfGroupObjectId, ObjectStatus::END_OF_GROUP, /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfTrackAndGroup(uint64_t endOfTrackObjectId) {
  return publishStatus(
      endOfTrackObjectId,
      ObjectStatus::END_OF_TRACK_AND_GROUP,
      /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::publishStatus(
    uint64_t objectID,
    ObjectStatus status,
    bool finStream) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  header_.status = status;
  return writeCurrentObject(
      objectID, /*length=*/0, /*payload=*/nullptr, finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfSubgroup() {
  if (currentLengthRemaining_) {
    XLOG(ERR) << "Still publishing previous object sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Previous object incomplete"));
  }
  if (!writeBuf_.empty()) {
    XLOG(WARN) << "No objects published on subgroup=" << header_;
  }
  return writeToStream(/*finStream=*/true);
}

void StreamPublisherImpl::reset(ResetStreamErrorCode error) {
  if (!writeBuf_.empty()) {
    // TODO: stream header is pending, reliable reset?
    XLOG(WARN) << "Stream header pending on subgroup=" << header_;
  }
  if (writeHandle_) {
    auto writeHandle = writeHandle_;
    writeHandle_ = nullptr;
    writeHandle->resetStream(uint32_t(error));
  } else {
    // Can happen on STOP_SENDING
    XLOG(ERR) << "reset with no write handle: sgp=" << this;
  }
  onStreamComplete();
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
StreamPublisherImpl::awaitReadyToConsume() {
  if (!writeHandle_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
  }
  auto writableFuture = writeHandle_->awaitWritable();
  if (!writableFuture) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::WRITE_ERROR, "awaitWritable failed"));
  }
  return std::move(writableFuture.value());
}

} // namespace

namespace moxygen {

// Receive State
class MoQSession::TrackReceiveStateBase {
 public:
  TrackReceiveStateBase(FullTrackName fullTrackName, SubscribeID subscribeID)
      : fullTrackName_(std::move(fullTrackName)), subscribeID_(subscribeID) {}

  ~TrackReceiveStateBase() = default;

  [[nodiscard]] const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  folly::CancellationToken getCancelToken() const {
    return cancelSource_.getToken();
  }

 protected:
  FullTrackName fullTrackName_;
  SubscribeID subscribeID_;
  folly::CancellationSource cancelSource_;
};

class MoQSession::SubscribeTrackReceiveState
    : public MoQSession::TrackReceiveStateBase {
 public:
  using SubscribeResult = MoQSession::SubscribeResult;
  SubscribeTrackReceiveState(
      FullTrackName fullTrackName,
      SubscribeID subscribeID,
      std::shared_ptr<TrackConsumer> callback)
      : TrackReceiveStateBase(std::move(fullTrackName), subscribeID),
        callback_(std::move(callback)) {}

  folly::coro::Future<SubscribeResult> subscribeFuture() {
    auto contract = folly::coro::makePromiseContract<SubscribeResult>();
    promise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  [[nodiscard]] const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  std::shared_ptr<TrackConsumer> getSubscribeCallback() const {
    return callback_;
  }

  void resetSubscribeCallback() {
    callback_.reset();
  }

  void cancel() {
    callback_.reset();
    cancelSource_.requestCancellation();
  }

  void subscribeOK(SubscribeOk subscribeOK) {
    promise_.setValue(std::move(subscribeOK));
  }

  void subscribeError(SubscribeError subErr) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (!promise_.isFulfilled()) {
      subErr.subscribeID = subscribeID_;
      promise_.setValue(folly::makeUnexpected(std::move(subErr)));
    } else {
      subscribeDone(
          {subscribeID_,
           SubscribeDoneStatusCode::SESSION_CLOSED,
           "closed locally",
           folly::none});
    }
  }

  void subscribeDone(SubscribeDone subDone) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (callback_) {
      callback_->subscribeDone(std::move(subDone));
    } // else, unsubscribe raced with subscribeDone and callback was removed
  }

 private:
  std::shared_ptr<TrackConsumer> callback_;
  folly::coro::Promise<SubscribeResult> promise_;
};

class MoQSession::FetchTrackReceiveState
    : public MoQSession::TrackReceiveStateBase {
 public:
  using FetchResult = folly::Expected<SubscribeID, FetchError>;
  FetchTrackReceiveState(
      FullTrackName fullTrackName,
      SubscribeID subscribeID,
      std::shared_ptr<FetchConsumer> fetchCallback)
      : TrackReceiveStateBase(std::move(fullTrackName), subscribeID),
        callback_(std::move(fetchCallback)) {}

  folly::coro::Future<FetchResult> fetchFuture() {
    auto contract = folly::coro::makePromiseContract<FetchResult>();
    promise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  std::shared_ptr<FetchConsumer> getFetchCallback() const {
    return callback_;
  }

  void resetFetchCallback(const std::shared_ptr<MoQSession>& session) {
    callback_.reset();
    if (fetchOkAndAllDataReceived()) {
      session->fetches_.erase(subscribeID_);
      session->checkForCloseOnDrain();
    }
  }

  void cancel(const std::shared_ptr<MoQSession>& session) {
    cancelSource_.requestCancellation();
    resetFetchCallback(session);
  }

  void fetchOK() {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    promise_.setValue(subscribeID_);
  }

  void fetchError(FetchError fetchErr) {
    if (!promise_.isFulfilled()) {
      fetchErr.subscribeID = subscribeID_;
      promise_.setValue(folly::makeUnexpected(std::move(fetchErr)));
    } // there's likely a missing case here from shutdown
  }

  bool fetchOkAndAllDataReceived() const {
    return promise_.isFulfilled() && !callback_;
  }

 private:
  std::shared_ptr<FetchConsumer> callback_;
  folly::coro::Promise<FetchResult> promise_;
};

using folly::coro::co_awaitTry;
using folly::coro::co_error;

MoQSession::~MoQSession() {
  cleanup();
}

void MoQSession::cleanup() {
  // TODO: Are these loops safe since they may (should?) delete elements
  for (auto& pubTrack : pubTracks_) {
    pubTrack.second->reset(ResetStreamErrorCode::SESSION_CLOSED);
  }
  pubTracks_.clear();
  for (auto& subTrack : subTracks_) {
    subTrack.second->subscribeError(
        {/*TrackReceiveState fills in subId*/ 0,
         500,
         "session closed",
         folly::none});
  }
  subTracks_.clear();
  for (auto& fetch : fetches_) {
    // TODO: there needs to be a way to queue an error in TrackReceiveState,
    // both from here, when close races the FETCH stream, and from readLoop
    // where we get a reset.
    fetch.second->fetchError(
        {/*TrackReceiveState fills in subId*/ 0, 500, "session closed"});
  }
  fetches_.clear();
  for (auto& [trackNamespace, pendingAnn] : pendingAnnounce_) {
    pendingAnn.setValue(folly::makeUnexpected(
        AnnounceError({trackNamespace, 500, "session closed"})));
  }
  pendingAnnounce_.clear();
  for (auto& [trackNamespace, pendingSn] : pendingSubscribeAnnounces_) {
    pendingSn.setValue(folly::makeUnexpected(
        SubscribeAnnouncesError({trackNamespace, 500, "session closed"})));
  }
  pendingSubscribeAnnounces_.clear();
  if (!cancellationSource_.isCancellationRequested()) {
    XLOG(DBG1) << "requestCancellation from cleanup sess=" << this;
    cancellationSource_.requestCancellation();
  }
}

void MoQSession::start() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    auto cs = wt_->createBidiStream();
    if (!cs) {
      XLOG(ERR) << "Failed to get control stream sess=" << this;
      wt_->closeSession();
      return;
    }
    auto controlStream = cs.value();
    controlStream.writeHandle->setPriority(0, 0, false);

    auto mergeToken = folly::CancellationToken::merge(
        cancellationSource_.getToken(),
        controlStream.writeHandle->getCancelToken());
    folly::coro::co_withCancellation(
        std::move(mergeToken), controlWriteLoop(controlStream.writeHandle))
        .scheduleOn(evb_)
        .start();
    co_withCancellation(
        cancellationSource_.getToken(),
        controlReadLoop(controlStream.readHandle))
        .scheduleOn(evb_)
        .start();
  }
}

void MoQSession::drain() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  draining_ = true;
  checkForCloseOnDrain();
}

void MoQSession::checkForCloseOnDrain() {
  if (draining_ && fetches_.empty() && subTracks_.empty()) {
    close(SessionCloseErrorCode::NO_ERROR);
  }
}

void MoQSession::close(SessionCloseErrorCode error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (wt_) {
    // TODO: The error code should be propagated to
    // whatever implemented proxygen::WebTransport.
    // TxnWebTransport current just ignores the errorCode
    auto wt = wt_;
    wt_ = nullptr;

    cleanup();

    wt->closeSession(folly::to_underlying(error));
    XLOG(DBG1) << "requestCancellation from close sess=" << this;
    cancellationSource_.requestCancellation();
  }
}

folly::coro::Task<void> MoQSession::controlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* controlStream) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  while (true) {
    co_await folly::coro::co_safe_point;
    if (controlWriteBuf_.empty()) {
      controlWriteEvent_.reset();
      auto res = co_await co_awaitTry(controlWriteEvent_.wait());
      if (res.tryGetExceptionObject<folly::FutureTimeout>()) {
      } else if (res.tryGetExceptionObject<folly::OperationCancelled>()) {
        co_return;
      } else if (res.hasException()) {
        XLOG(ERR) << "Unexpected exception: "
                  << folly::exceptionStr(res.exception());
        co_return;
      }
    }
    co_await folly::coro::co_safe_point;
    auto writeRes =
        controlStream->writeStreamData(controlWriteBuf_.move(), false);
    if (!writeRes) {
      XLOG(ERR) << "Write error: " << uint64_t(writeRes.error());
      break;
    }
    auto awaitRes = controlStream->awaitWritable();
    if (!awaitRes) {
      XLOG(ERR) << "Control stream write error";
      break;
    }
    co_await std::move(*awaitRes);
  }
}

folly::coro::Task<ServerSetup> MoQSession::setup(ClientSetup setup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;
  std::tie(setupPromise_, setupFuture_) =
      folly::coro::makePromiseContract<ServerSetup>();

  auto maxSubscribeId = getMaxSubscribeIdIfPresent(setup.params);
  auto res = writeClientSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeClientSetup failed sess=" << this;
    co_yield folly::coro::co_error(std::runtime_error("Failed to write setup"));
  }
  maxSubscribeID_ = maxConcurrentSubscribes_ = maxSubscribeId;
  controlWriteEvent_.signal();

  auto deletedToken = cancellationSource_.getToken();
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::CancellationToken::merge(deletedToken, token);
  folly::EventBaseThreadTimekeeper tk(*evb_);
  auto serverSetup = co_await co_awaitTry(folly::coro::co_withCancellation(
      mergeToken,
      folly::coro::timeout(std::move(setupFuture_), kSetupTimeout, &tk)));
  if (mergeToken.isCancellationRequested()) {
    co_yield folly::coro::co_error(folly::OperationCancelled());
  }
  if (serverSetup.hasException()) {
    close(SessionCloseErrorCode::INTERNAL_ERROR);
    XLOG(ERR) << "Setup Failed: "
              << folly::exceptionStr(serverSetup.exception());
    co_yield folly::coro::co_error(serverSetup.exception());
  }
  setupComplete_ = true;
  co_return *serverSetup;
}

void MoQSession::onServerSetup(ServerSetup serverSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (serverSetup.selectedVersion != kVersionDraftCurrent) {
    XLOG(ERR) << "Invalid version = " << serverSetup.selectedVersion
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    setupPromise_.setException(std::runtime_error("Invalid version"));
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(serverSetup.params);
  setupPromise_.setValue(std::move(serverSetup));
}

void MoQSession::onClientSetup(ClientSetup clientSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (std::find(
          clientSetup.supportedVersions.begin(),
          clientSetup.supportedVersions.end(),
          kVersionDraftCurrent) == clientSetup.supportedVersions.end()) {
    XLOG(ERR) << "No matching versions sess=" << this;
    for (auto v : clientSetup.supportedVersions) {
      XLOG(ERR) << "client sent=" << v << " sess=" << this;
    }
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(clientSetup.params);
  auto serverSetup =
      serverSetupCallback_->onClientSetup(std::move(clientSetup));
  if (!serverSetup.hasValue()) {
    XLOG(ERR) << "Server setup callback failed sess=" << this;
    close(SessionCloseErrorCode::INTERNAL_ERROR);
    return;
  }

  auto maxSubscribeId = getMaxSubscribeIdIfPresent(serverSetup->params);
  auto res = writeServerSetup(controlWriteBuf_, std::move(*serverSetup));
  if (!res) {
    XLOG(ERR) << "writeServerSetup failed sess=" << this;
    return;
  }
  maxSubscribeID_ = maxConcurrentSubscribes_ = maxSubscribeId;
  setupComplete_ = true;
  setupPromise_.setValue(ServerSetup());
  controlWriteEvent_.signal();
}

folly::coro::AsyncGenerator<MoQSession::MoQMessage>
MoQSession::controlMessages() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto self = shared_from_this();
  while (true) {
    auto token = cancellationSource_.getToken();
    auto message = co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(token, controlMessages_.dequeue()));
    if (token.isCancellationRequested()) {
      co_return;
    }
    if (message.hasException()) {
      XLOG(ERR) << folly::exceptionStr(message.exception()) << " sess=" << this;
      break;
    }
    co_yield *message;
  }
}

folly::coro::Task<void> MoQSession::controlReadLoop(
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  MoQControlCodec codec(dir_, this);
  auto streamId = readHandle->getID();
  codec.setStreamId(streamId);

  bool fin = false;
  auto token = co_await folly::coro::co_current_cancellation_token;
  while (!fin && !token.isCancellationRequested()) {
    auto streamData = co_await folly::coro::co_awaitTry(
        readHandle->readStreamData().via(evb_));
    if (streamData.hasException()) {
      XLOG(ERR) << folly::exceptionStr(streamData.exception())
                << " id=" << streamId << " sess=" << this;
      break;
    } else {
      if (streamData->data || streamData->fin) {
        try {
          codec.onIngress(std::move(streamData->data), streamData->fin);
        } catch (const std::exception& ex) {
          XLOG(FATAL) << "exception thrown from onIngress ex="
                      << folly::exceptionStr(ex);
        }
      }
      fin = streamData->fin;
      XLOG_IF(DBG3, fin) << "End of stream id=" << streamId << " sess=" << this;
    }
  }
  // TODO: close session on control exit
}

std::shared_ptr<MoQSession::SubscribeTrackReceiveState>
MoQSession::getSubscribeTrackReceiveState(TrackAlias trackAlias) {
  auto trackIt = subTracks_.find(trackAlias);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown track alias
    XLOG(ERR) << "unknown track alias=" << trackAlias << " sess=" << this;
    return nullptr;
  }
  return trackIt->second;
}

std::shared_ptr<MoQSession::FetchTrackReceiveState>
MoQSession::getFetchTrackReceiveState(SubscribeID subscribeID) {
  XLOG(DBG3) << "getTrack subID=" << subscribeID;
  auto trackIt = fetches_.find(subscribeID);
  if (trackIt == fetches_.end()) {
    // received an object for unknown subscribe ID
    XLOG(ERR) << "unknown subscribe ID=" << subscribeID << " sess=" << this;
    return nullptr;
  }
  return trackIt->second;
}

namespace {
class ObjectStreamCallback : public MoQObjectStreamCodec::ObjectCallback {
  // TODO: MoQConsumers should have a "StreamConsumer" that both
  // SubgroupConsumer and FetchConsumer can inherit.  In that case we can
  // get rid of these templates.  It will also be easier for publishers.

  template <typename SubscribeMethod, typename FetchMethod, typename... Args>
  auto invokeCallback(
      SubscribeMethod smethod,
      FetchMethod fmethod,
      uint64_t groupID,
      uint64_t subgroupID,
      Args... args) {
    if (fetchState_) {
      return (fetchState_->getFetchCallback().get()->*fmethod)(
          groupID, subgroupID, std::forward<Args>(args)...);
    } else {
      return (subgroupCallback_.get()->*smethod)(std::forward<Args>(args)...);
    }
  }

  template <typename SubscribeMethod, typename FetchMethod, typename... Args>
  auto invokeCallbackNoGroup(
      SubscribeMethod smethod,
      FetchMethod fmethod,
      Args... args) {
    if (fetchState_) {
      return (fetchState_->getFetchCallback().get()->*fmethod)(
          std::forward<Args>(args)...);
    } else {
      return (subgroupCallback_.get()->*smethod)(std::forward<Args>(args)...);
    }
  }

 public:
  ObjectStreamCallback(
      std::shared_ptr<MoQSession> session,
      folly::CancellationToken& token)
      : session_(session), token_(token) {}

  void onSubgroup(
      TrackAlias alias,
      uint64_t group,
      uint64_t subgroup,
      Priority priority) override {
    subscribeState_ = session_->getSubscribeTrackReceiveState(alias);
    if (!subscribeState_) {
      error_ = MoQPublishError(
          MoQPublishError::CANCELLED, "Subgroup for unknown track");
      return;
    }
    token_ = folly::CancellationToken::merge(
        token_, subscribeState_->getCancelToken());
    auto callback = subscribeState_->getSubscribeCallback();
    if (!callback) {
      return;
    }
    auto res = callback->beginSubgroup(group, subgroup, priority);
    if (res.hasValue()) {
      subgroupCallback_ = *res;
    } else {
      error_ = std::move(res.error());
    }
  }

  void onFetchHeader(SubscribeID subscribeID) override {
    fetchState_ = session_->getFetchTrackReceiveState(subscribeID);

    if (!fetchState_) {
      error_ = MoQPublishError(
          MoQPublishError::CANCELLED, "Fetch response for unknown track");
      return;
    }
    token_ =
        folly::CancellationToken::merge(token_, fetchState_->getCancelToken());
  }

  void onObjectBegin(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      bool objectComplete,
      bool streamComplete) override {
    if (isCancelled()) {
      return;
    }

    folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
    if (objectComplete) {
      res = invokeCallback(
          &SubgroupConsumer::object,
          &FetchConsumer::object,
          group,
          subgroup,
          objectID,
          std::move(initialPayload),
          streamComplete);
      if (streamComplete) {
        endOfSubgroup();
      }
    } else {
      res = invokeCallback(
          &SubgroupConsumer::beginObject,
          &FetchConsumer::beginObject,
          group,
          subgroup,
          objectID,
          length,
          std::move(initialPayload));
    }
    if (!res) {
      error_ = std::move(res.error());
    }
  }

  void onObjectPayload(Payload payload, bool objectComplete) override {
    if (isCancelled()) {
      return;
    }

    bool finStream = false;
    auto res = invokeCallbackNoGroup(
        &SubgroupConsumer::objectPayload,
        &FetchConsumer::objectPayload,
        std::move(payload),
        finStream);
    if (!res) {
      error_ = std::move(res.error());
    } else {
      XCHECK_EQ(objectComplete, res.value() == ObjectPublishStatus::DONE);
    }
  }

  void onObjectStatus(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      ObjectStatus status) override {
    if (isCancelled()) {
      return;
    }
    folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
    switch (status) {
      case ObjectStatus::NORMAL:
        break;
      case ObjectStatus::OBJECT_NOT_EXIST:
        res = invokeCallback(
            &SubgroupConsumer::objectNotExists,
            &FetchConsumer::objectNotExists,
            group,
            subgroup,
            objectID,
            false);
        break;
      case ObjectStatus::GROUP_NOT_EXIST:
        // groupNotExists is on the TrackConsumer not SubgroupConsumer
        if (fetchState_) {
          res = fetchState_->getFetchCallback()->groupNotExists(
              group, subgroup, false);
        } else {
          res = subscribeState_->getSubscribeCallback()->groupNotExists(
              group, subgroup, true);
          endOfSubgroup();
        }
        break;
      case ObjectStatus::END_OF_GROUP:
        // FetchConsumer::endOfGroup has an optional param
        if (fetchState_) {
          res = fetchState_->getFetchCallback()->endOfGroup(
              group,
              subgroup,
              objectID,
              /*finFetch=*/false);
        } else {
          res = subgroupCallback_->endOfGroup(objectID);
          endOfSubgroup();
        }
        break;
      case ObjectStatus::END_OF_TRACK_AND_GROUP:
        res = invokeCallback(
            &SubgroupConsumer::endOfTrackAndGroup,
            &FetchConsumer::endOfTrackAndGroup,
            group,
            subgroup,
            objectID);
        endOfSubgroup();
        break;
      case ObjectStatus::END_OF_SUBGROUP:
        endOfSubgroup(/*deliverCallback=*/true);
        break;
    }
    if (!res) {
      error_ = std::move(res.error());
    }
  }

  void onEndOfStream() override {
    if (!isCancelled()) {
      endOfSubgroup(/*deliverCallback=*/true);
    }
  }

  void onConnectionError(ErrorCode error) override {
    XLOG(ERR) << "Parse error=" << folly::to_underlying(error);
    session_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
  }

  // Called by read loop on read error (eg: RESET_STREAM)
  bool reset(ResetStreamErrorCode error) {
    if (!subscribeState_ && !fetchState_) {
      return false;
    }
    if (!isCancelled()) {
      // ignoring error from reset?
      invokeCallbackNoGroup(
          &SubgroupConsumer::reset, &FetchConsumer::reset, error);
    }
    endOfSubgroup();
    return true;
  }

  folly::Optional<MoQPublishError> error() const {
    return error_;
  }

 private:
  bool isCancelled() const {
    if (fetchState_) {
      return !fetchState_->getFetchCallback();
    } else if (subscribeState_) {
      return !subgroupCallback_ || !subscribeState_->getSubscribeCallback();
    } else {
      return true;
    }
  }

  void endOfSubgroup(bool deliverCallback = false) {
    if (deliverCallback && !isCancelled()) {
      if (fetchState_) {
        fetchState_->getFetchCallback()->endOfFetch();
      } else {
        subgroupCallback_->endOfSubgroup();
      }
    }
    if (fetchState_) {
      fetchState_->resetFetchCallback(session_);
    } else {
      subgroupCallback_.reset();
    }
  }

  std::shared_ptr<MoQSession> session_;
  folly::CancellationToken& token_;
  std::shared_ptr<MoQSession::SubscribeTrackReceiveState> subscribeState_;
  std::shared_ptr<SubgroupConsumer> subgroupCallback_;
  std::shared_ptr<MoQSession::FetchTrackReceiveState> fetchState_;
  folly::Optional<MoQPublishError> error_;
};
} // namespace

folly::coro::Task<void> MoQSession::unidirectionalReadLoop(
    std::shared_ptr<MoQSession> session,
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  auto id = readHandle->getID();
  XLOG(DBG1) << __func__ << " id=" << id << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this, id] {
    XLOG(DBG1) << "exit " << func << " id=" << id << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  auto token = co_await folly::coro::co_current_cancellation_token;
  MoQObjectStreamCodec codec(nullptr);
  ObjectStreamCallback dcb(session, /*by ref*/ token);
  codec.setCallback(&dcb);
  codec.setStreamId(id);

  bool fin = false;
  while (!fin && !token.isCancellationRequested()) {
    auto streamData =
        co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
            token,
            folly::coro::toTaskInterruptOnCancel(
                readHandle->readStreamData().via(evb_))));
    if (streamData.hasException()) {
      XLOG(ERR) << folly::exceptionStr(streamData.exception()) << " id=" << id
                << " sess=" << this;
      ResetStreamErrorCode errorCode{ResetStreamErrorCode::INTERNAL_ERROR};
      auto wtEx =
          streamData.tryGetExceptionObject<proxygen::WebTransport::Exception>();
      if (wtEx) {
        errorCode = ResetStreamErrorCode(wtEx->error);
      } else {
        XLOG(ERR) << folly::exceptionStr(streamData.exception());
      }
      if (!dcb.reset(errorCode)) {
        XLOG(ERR) << __func__ << " terminating for unknown "
                  << "stream id=" << id << " sess=" << this;
      }
      break;
    } else {
      if (streamData->data || streamData->fin) {
        fin = streamData->fin;
        folly::Optional<MoQPublishError> err;
        try {
          codec.onIngress(std::move(streamData->data), streamData->fin);
          err = dcb.error();
        } catch (const std::exception& ex) {
          err = MoQPublishError(
              MoQPublishError::CANCELLED,
              folly::exceptionStr(ex).toStdString());
        }
        XLOG_IF(DBG3, fin) << "End of stream id=" << id << " sess=" << this;
        if (err) {
          XLOG(ERR) << "Error parsing/consuming stream, " << err->describe()
                    << " id=" << id << " sess=" << this;
          if (!fin) {
            readHandle->stopSending(/*error=*/0);
            break;
          }
        }
      } // else empty read
    }
  }
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  XLOG(DBG1) << __func__ << " ftn=" << subscribeRequest.fullTrackName
             << " sess=" << this;
  const auto subscribeID = subscribeRequest.subscribeID;
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }

  // TODO: The publisher should maintain some state like
  //   Subscribe ID -> Track Name, Locations [currently held in
  //   MoQForwarder] Track Alias -> Track Name
  // If ths session holds this state, it can check for duplicate
  // subscriptions
  auto it = pubTracks_.find(subscribeRequest.subscribeID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << subscribeRequest.subscribeID
              << " sess=" << this;
    subscribeError({subscribeRequest.subscribeID, 400, "dup sub ID"});
    return;
  }
  // TODO: Check for duplicate alias
  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      subscribeRequest.subscribeID,
      subscribeRequest.trackAlias,
      subscribeRequest.priority,
      subscribeRequest.groupOrder);
  pubTracks_.emplace(subscribeID, std::move(trackPublisher));
  // TODO: there should be a timeout for the application to call
  // subscribeOK/Error
  controlMessages_.enqueue(std::move(subscribeRequest));
}

void MoQSession::onSubscribeUpdate(SubscribeUpdate subscribeUpdate) {
  XLOG(DBG1) << __func__ << " id=" << subscribeUpdate.subscribeID
             << " sess=" << this;
  const auto subscribeID = subscribeUpdate.subscribeID;
  auto it = pubTracks_.find(subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << subscribeID << " sess=" << this;
    return;
  }
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }

  it->second->setSubPriority(subscribeUpdate.priority);
  // TODO: update priority of tracks in flight
  controlMessages_.enqueue(std::move(subscribeUpdate));
}

void MoQSession::onUnsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " id=" << unsubscribe.subscribeID
             << " sess=" << this;
  // How does this impact pending subscribes?
  // and open TrackReceiveStates
  controlMessages_.enqueue(std::move(unsubscribe));
}

void MoQSession::onSubscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " id=" << subOk.subscribeID << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subOk.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subOk.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackReceiveStateIt = subTracks_.find(trackAliasIt->second);
  if (trackReceiveStateIt != subTracks_.end()) {
    trackReceiveStateIt->second->subscribeOK(std::move(subOk));
  } else {
    XLOG(ERR) << "Missing subTracks_ entry for alias=" << trackAliasIt->second;
  }
}

void MoQSession::onSubscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " id=" << subErr.subscribeID << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subErr.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subErr.subscribeID
              << " sess=" << this;
    return;
  }

  auto trackReceiveStateIt = subTracks_.find(trackAliasIt->second);
  if (trackReceiveStateIt != subTracks_.end()) {
    trackReceiveStateIt->second->subscribeError(std::move(subErr));
    subTracks_.erase(trackReceiveStateIt);
    subIdToTrackAlias_.erase(trackAliasIt);
    checkForCloseOnDrain();
  } else {
    XLOG(ERR) << "Missing subTracks_ entry for alias=" << trackAliasIt->second;
  }
}

void MoQSession::onSubscribeDone(SubscribeDone subscribeDone) {
  XLOG(DBG1) << "SubscribeDone id=" << subscribeDone.subscribeID
             << " code=" << folly::to_underlying(subscribeDone.statusCode)
             << " reason=" << subscribeDone.reasonPhrase;
  auto trackAliasIt = subIdToTrackAlias_.find(subscribeDone.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subscribeDone.subscribeID
              << " sess=" << this;
    return;
  }

  // TODO: handle final object and status code
  // TODO: there could still be objects in flight.  Removing from maps now
  // will prevent their delivery.  I think the only way to handle this is with
  // timeouts.
  auto trackReceiveStateIt = subTracks_.find(trackAliasIt->second);
  if (trackReceiveStateIt != subTracks_.end()) {
    auto state = trackReceiveStateIt->second;
    subTracks_.erase(trackReceiveStateIt);
    state->subscribeDone(std::move(subscribeDone));
  } else {
    XLOG(DFATAL) << "trackAliasIt but no trackReceiveStateIt for id="
                 << subscribeDone.subscribeID << " sess=" << this;
  }
  subIdToTrackAlias_.erase(trackAliasIt);
  checkForCloseOnDrain();
}

void MoQSession::onMaxSubscribeId(MaxSubscribeId maxSubscribeId) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (maxSubscribeId.subscribeID.value > peerMaxSubscribeID_) {
    XLOG(DBG1) << fmt::format(
        "Bumping the maxSubscribeId to: {} from: {}",
        maxSubscribeId.subscribeID.value,
        peerMaxSubscribeID_);
    peerMaxSubscribeID_ = maxSubscribeId.subscribeID.value;
    return;
  }

  XLOG(ERR) << fmt::format(
      "Invalid MaxSubscribeId: {}. Current maxSubscribeId:{}",
      maxSubscribeId.subscribeID.value,
      maxSubscribeID_);
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

void MoQSession::onFetch(Fetch fetch) {
  XLOG(DBG1) << __func__ << " ftn=" << fetch.fullTrackName << " sess=" << this;
  const auto subscribeID = fetch.subscribeID;
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }
  if (fetch.end < fetch.start) {
    fetchError(
        {fetch.subscribeID,
         folly::to_underlying(FetchErrorCode::INVALID_RANGE),
         "End must be after start"});
    return;
  }
  auto it = pubTracks_.find(fetch.subscribeID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << fetch.subscribeID
              << " sess=" << this;
    fetchError({fetch.subscribeID, 400, "dup sub ID"});
    return;
  }
  auto fetchPublisher = std::make_shared<FetchPublisherImpl>(
      this, fetch.subscribeID, fetch.priority, fetch.groupOrder);
  pubTracks_.emplace(fetch.subscribeID, std::move(fetchPublisher));
  controlMessages_.enqueue(std::move(fetch));
}

void MoQSession::onFetchCancel(FetchCancel fetchCancel) {
  XLOG(DBG1) << __func__ << " id=" << fetchCancel.subscribeID
             << " sess=" << this;
  auto pubTrackIt = pubTracks_.find(fetchCancel.subscribeID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(DBG4) << "No publish key for fetch id=" << fetchCancel.subscribeID
               << " sess=" << this;
    // The Fetch stream has already closed, or never existed
    // If it's already closed, a no-op is fine.
    // See: https://github.com/moq-wg/moq-transport/issues/630
  } else {
    // It's possible the fetch stream hasn't opened yet if the application
    // hasn't made it to fetchOK.
    pubTrackIt->second->reset(ResetStreamErrorCode::CANCELLED);
    retireSubscribeId(/*signalWriteLoop=*/true);
  }
}

void MoQSession::onFetchOk(FetchOk fetchOk) {
  XLOG(DBG1) << __func__ << " id=" << fetchOk.subscribeID << " sess=" << this;
  auto fetchIt = fetches_.find(fetchOk.subscribeID);
  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchOk.subscribeID
              << " sess=" << this;
    return;
  }
  const auto& trackReceiveState = fetchIt->second;
  trackReceiveState->fetchOK();
  if (trackReceiveState->fetchOkAndAllDataReceived()) {
    fetches_.erase(fetchIt);
    checkForCloseOnDrain();
  }
}

void MoQSession::onFetchError(FetchError fetchError) {
  XLOG(DBG1) << __func__ << " id=" << fetchError.subscribeID
             << " sess=" << this;
  auto fetchIt = fetches_.find(fetchError.subscribeID);
  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchError.subscribeID
              << " sess=" << this;
    return;
  }
  fetchIt->second->fetchError(fetchError);
  fetches_.erase(fetchIt);
  checkForCloseOnDrain();
}

void MoQSession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;
  controlMessages_.enqueue(std::move(ann));
}

void MoQSession::onAnnounceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__ << " ns=" << annOk.trackNamespace << " sess=" << this;
  auto annIt = pendingAnnounce_.find(annOk.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace=" << annOk.trackNamespace
              << " sess=" << this;
    return;
  }
  annIt->second.setValue(std::move(annOk));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onAnnounceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " ns=" << announceError.trackNamespace
             << " sess=" << this;
  auto annIt = pendingAnnounce_.find(announceError.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace="
              << announceError.trackNamespace << " sess=" << this;
    return;
  }
  annIt->second.setValue(folly::makeUnexpected(std::move(announceError)));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onUnannounce(Unannounce unAnn) {
  XLOG(DBG1) << __func__ << " ns=" << unAnn.trackNamespace << " sess=" << this;
  controlMessages_.enqueue(std::move(unAnn));
}

void MoQSession::onAnnounceCancel(AnnounceCancel announceCancel) {
  XLOG(DBG1) << __func__ << " ns=" << announceCancel.trackNamespace
             << " sess=" << this;
  controlMessages_.enqueue(std::move(announceCancel));
}

void MoQSession::onSubscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  controlMessages_.enqueue(std::move(sa));
}

void MoQSession::onSubscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
  XLOG(DBG1) << __func__ << " prefix=" << saOk.trackNamespacePrefix
             << " sess=" << this;
  auto saIt = pendingSubscribeAnnounces_.find(saOk.trackNamespacePrefix);
  if (saIt == pendingSubscribeAnnounces_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces trackNamespace="
              << saOk.trackNamespacePrefix << " sess=" << this;
    return;
  }
  saIt->second.setValue(std::move(saOk));
  pendingSubscribeAnnounces_.erase(saIt);
}

void MoQSession::onSubscribeAnnouncesError(
    SubscribeAnnouncesError subscribeAnnouncesError) {
  XLOG(DBG1) << __func__
             << " prefix=" << subscribeAnnouncesError.trackNamespacePrefix
             << " sess=" << this;
  auto saIt = pendingSubscribeAnnounces_.find(
      subscribeAnnouncesError.trackNamespacePrefix);
  if (saIt == pendingSubscribeAnnounces_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces trackNamespace="
              << subscribeAnnouncesError.trackNamespacePrefix
              << " sess=" << this;
    return;
  }
  saIt->second.setValue(
      folly::makeUnexpected(std::move(subscribeAnnouncesError)));
  pendingSubscribeAnnounces_.erase(saIt);
}

void MoQSession::onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) {
  XLOG(DBG1) << __func__ << " prefix=" << unsub.trackNamespacePrefix
             << " sess=" << this;
  controlMessages_.enqueue(std::move(unsub));
}

void MoQSession::onTrackStatusRequest(TrackStatusRequest trackStatusRequest) {
  XLOG(DBG1) << __func__ << " ftn=" << trackStatusRequest.fullTrackName
             << " sess=" << this;
  controlMessages_.enqueue(std::move(trackStatusRequest));
}

void MoQSession::onTrackStatus(TrackStatus trackStatus) {
  XLOG(DBG1) << __func__ << " ftn=" << trackStatus.fullTrackName
             << " code=" << uint64_t(trackStatus.statusCode)
             << " sess=" << this;
  controlMessages_.enqueue(std::move(trackStatus));
}

void MoQSession::onGoaway(Goaway goaway) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(goaway));
}

void MoQSession::onConnectionError(ErrorCode error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  XLOG(ERR) << "MoQCodec control stream parse error err="
            << folly::to_underlying(error);
  // TODO: This error is coming from MoQCodec -- do we need a better
  // error code?
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

folly::coro::Task<folly::Expected<AnnounceOk, AnnounceError>>
MoQSession::announce(Announce ann) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;
  auto trackNamespace = ann.trackNamespace;
  auto res = writeAnnounce(controlWriteBuf_, std::move(ann));
  if (!res) {
    XLOG(ERR) << "writeAnnounce failed sess=" << this;
    co_return folly::makeUnexpected(
        AnnounceError({std::move(trackNamespace), 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<AnnounceOk, AnnounceError>>();
  pendingAnnounce_.emplace(
      std::move(trackNamespace), std::move(contract.first));
  co_return co_await std::move(contract.second);
}

void MoQSession::announceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__ << " ns=" << annOk.trackNamespace << " sess=" << this;
  auto res = writeAnnounceOk(controlWriteBuf_, std::move(annOk));
  if (!res) {
    XLOG(ERR) << "writeAnnounceOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::announceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " ns=" << announceError.trackNamespace
             << " sess=" << this;
  auto res = writeAnnounceError(controlWriteBuf_, std::move(announceError));
  if (!res) {
    XLOG(ERR) << "writeAnnounceError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unannounce(Unannounce unann) {
  XLOG(DBG1) << __func__ << " ns=" << unann.trackNamespace << " sess=" << this;
  auto trackNamespace = unann.trackNamespace;
  auto res = writeUnannounce(controlWriteBuf_, std::move(unann));
  if (!res) {
    XLOG(ERR) << "writeUnannounce failed sess=" << this;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<
    folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
MoQSession::subscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  auto trackNamespace = sa.trackNamespacePrefix;
  auto res = writeSubscribeAnnounces(controlWriteBuf_, std::move(sa));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnounces failed sess=" << this;
    co_return folly::makeUnexpected(SubscribeAnnouncesError(
        {std::move(trackNamespace), 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>();
  pendingSubscribeAnnounces_.emplace(
      std::move(trackNamespace), std::move(contract.first));
  co_return co_await std::move(contract.second);
}

void MoQSession::subscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
  XLOG(DBG1) << __func__ << " prefix=" << saOk.trackNamespacePrefix
             << " sess=" << this;
  auto res = writeSubscribeAnnouncesOk(controlWriteBuf_, std::move(saOk));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeAnnouncesError(
    SubscribeAnnouncesError subscribeAnnouncesError) {
  XLOG(DBG1) << __func__
             << " prefix=" << subscribeAnnouncesError.trackNamespacePrefix
             << " sess=" << this;
  auto res = writeSubscribeAnnouncesError(
      controlWriteBuf_, std::move(subscribeAnnouncesError));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<MoQSession::SubscribeResult> MoQSession::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto fullTrackName = sub.fullTrackName;
  if (nextSubscribeID_ >= peerMaxSubscribeID_) {
    XLOG(WARN) << "Issuing subscribe that will fail; nextSubscribeID_="
               << nextSubscribeID_
               << " peerMaxSubscribeID_=" << peerMaxSubscribeID_
               << " sess=" << this;
  }
  SubscribeID subID = nextSubscribeID_++;
  sub.subscribeID = subID;
  sub.trackAlias = TrackAlias(subID.value);
  TrackAlias trackAlias = sub.trackAlias;
  auto wres = writeSubscribeRequest(controlWriteBuf_, std::move(sub));
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed sess=" << this;
    co_return folly::makeUnexpected(
        SubscribeError({subID, 500, "local write failed", folly::none}));
  }
  controlWriteEvent_.signal();
  auto res = subIdToTrackAlias_.emplace(subID, trackAlias);
  XCHECK(res.second) << "Duplicate subscribe ID";
  auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
      fullTrackName, subID, callback);
  auto subTrack = subTracks_.try_emplace(trackAlias, trackReceiveState);
  XCHECK(subTrack.second) << "Track alias already in use alias=" << trackAlias
                          << " sess=" << this;

  auto res2 = co_await trackReceiveState->subscribeFuture();
  XLOG(DBG1) << "Subscribe ready trackReceiveState=" << trackReceiveState
             << " subscribeID=" << subID;
  if (res2.hasValue()) {
    co_return std::move(res2.value());
  } else {
    co_return folly::makeUnexpected(res2.error());
  }
}

std::shared_ptr<TrackConsumer> MoQSession::subscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subOk.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subOk.subscribeID;
    return nullptr;
  }
  auto trackPublisher =
      std::dynamic_pointer_cast<TrackPublisherImpl>(it->second);
  if (!trackPublisher) {
    XLOG(ERR) << "subscribe ID maps to a fetch, not a subscribe, id="
              << subOk.subscribeID;
    subscribeError(
        {subOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }
  trackPublisher->setGroupOrder(subOk.groupOrder);
  auto res = writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed sess=" << this;
    return nullptr;
  }
  controlWriteEvent_.signal();
  return std::static_pointer_cast<TrackConsumer>(trackPublisher);
}

void MoQSession::subscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subErr.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subErr.subscribeID;
    return;
  }
  pubTracks_.erase(it);
  auto res = writeSubscribeError(controlWriteBuf_, std::move(subErr));
  retireSubscribeId(/*signalWriteLoop=*/false);
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(unsubscribe.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.subscribeID
              << " sess=" << this;
    return;
  }
  // no more callbacks after unsubscribe
  XLOG(DBG1) << "unsubscribing from ftn=" << trackIt->second->fullTrackName()
             << " sess=" << this;
  // if there are open streams for this subscription, we should STOP_SENDING
  // them?
  trackIt->second->cancel();
  auto res = writeUnsubscribe(controlWriteBuf_, std::move(unsubscribe));
  if (!res) {
    XLOG(ERR) << "writeUnsubscribe failed sess=" << this;
    return;
  }
  // we rely on receiving subscribeDone after unsubscribe to remove from
  // subTracks_
  controlWriteEvent_.signal();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::PublisherImpl::subscribeDone(SubscribeDone subscribeDone) {
  auto session = session_;
  session_ = nullptr;
  session->subscribeDone(std::move(subscribeDone));
  return folly::unit;
}

void MoQSession::subscribeDone(SubscribeDone subDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subDone.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "subscribeDone for invalid id=" << subDone.subscribeID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);
  auto res = writeSubscribeDone(controlWriteBuf_, std::move(subDone));
  if (!res) {
    XLOG(ERR) << "writeSubscribeDone failed sess=" << this;
    // TODO: any control write failure should probably result in close()
    return;
  }

  retireSubscribeId(/*signalWriteLoop=*/false);
  controlWriteEvent_.signal();
}

void MoQSession::retireSubscribeId(bool signalWriteLoop) {
  // If # of closed subscribes is greater than 1/2 of max subscribes, then
  // let's bump the maxSubscribeID by the number of closed subscribes.
  if (++closedSubscribes_ >= maxConcurrentSubscribes_ / 2) {
    maxSubscribeID_ += closedSubscribes_;
    closedSubscribes_ = 0;
    sendMaxSubscribeID(signalWriteLoop);
  }
}

void MoQSession::sendMaxSubscribeID(bool signalWriteLoop) {
  XLOG(DBG1) << "Issuing new maxSubscribeID=" << maxSubscribeID_
             << " sess=" << this;
  auto res =
      writeMaxSubscribeId(controlWriteBuf_, {.subscribeID = maxSubscribeID_});
  if (!res) {
    XLOG(ERR) << "writeMaxSubscribeId failed sess=" << this;
    return;
  }
  if (signalWriteLoop) {
    controlWriteEvent_.signal();
  }
}

void MoQSession::PublisherImpl::fetchComplete() {
  auto session = session_;
  session_ = nullptr;
  session->fetchComplete(subscribeID_);
}

void MoQSession::fetchComplete(SubscribeID subscribeID) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "fetchComplete for invalid id=" << subscribeID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);
  retireSubscribeId(/*signalWriteLoop=*/true);
}

void MoQSession::subscribeUpdate(SubscribeUpdate subUpdate) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subUpdate.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subUpdate.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subUpdate.subscribeID
              << " sess=" << this;
    return;
  }
  auto res = writeSubscribeUpdate(controlWriteBuf_, std::move(subUpdate));
  if (!res) {
    XLOG(ERR) << "writeSubscribeUpdate failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<folly::Expected<SubscribeID, FetchError>> MoQSession::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(DBG1) << "exit " << func; });
  auto fullTrackName = fetch.fullTrackName;
  if (nextSubscribeID_ >= peerMaxSubscribeID_) {
    XLOG(WARN) << "Issuing fetch that will fail; nextSubscribeID_="
               << nextSubscribeID_
               << " peerMaxSubscribeid_=" << peerMaxSubscribeID_
               << " sess=" << this;
  }
  auto subID = nextSubscribeID_++;
  fetch.subscribeID = subID;
  auto wres = writeFetch(controlWriteBuf_, std::move(fetch));
  if (!wres) {
    XLOG(ERR) << "writeFetch failed sess=" << this;
    co_return folly::makeUnexpected(
        FetchError({subID, 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto trackReceiveState = std::make_shared<FetchTrackReceiveState>(
      fullTrackName, subID, fetchCallback);
  auto fetchTrack = fetches_.try_emplace(subID, trackReceiveState);
  XCHECK(fetchTrack.second)
      << "SubscribeID already in use id=" << subID << " sess=" << this;
  auto res = co_await trackReceiveState->fetchFuture();
  XLOG(DBG1) << __func__
             << " fetchReady trackReceiveState=" << trackReceiveState;
  co_return res;
}

std::shared_ptr<FetchConsumer> MoQSession::fetchOk(FetchOk fetchOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(fetchOk.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Fetch OK, id=" << fetchOk.subscribeID;
    return nullptr;
  }
  auto fetchPublisher = dynamic_cast<FetchPublisherImpl*>(it->second.get());
  if (!fetchPublisher) {
    XLOG(ERR) << "subscribe ID maps to a subscribe, not a fetch, id="
              << fetchOk.subscribeID;
    fetchError(
        {fetchOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }
  auto fetchConsumer = fetchPublisher->beginFetch(fetchOk.groupOrder);
  if (!fetchConsumer) {
    XLOG(ERR) << "beginFetch Failed, id=" << fetchOk.subscribeID;
    fetchError(
        {fetchOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }

  auto res = writeFetchOk(controlWriteBuf_, fetchOk);
  if (!res) {
    XLOG(ERR) << "writeFetchOk failed sess=" << this;
    return nullptr;
  }
  controlWriteEvent_.signal();
  return *fetchConsumer;
}

void MoQSession::fetchError(FetchError fetchErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (pubTracks_.erase(fetchErr.subscribeID) == 0) {
    // fetchError is called sometimes before adding publisher state, so this
    // is not an error
    XLOG(DBG1) << "fetchErr for invalid id=" << fetchErr.subscribeID
               << " sess=" << this;
  }
  auto res = writeFetchError(controlWriteBuf_, std::move(fetchErr));
  if (!res) {
    XLOG(ERR) << "writeFetchError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::fetchCancel(FetchCancel fetchCan) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackIt = fetches_.find(fetchCan.subscribeID);
  if (trackIt == fetches_.end()) {
    XLOG(ERR) << "unknown subscribe ID=" << fetchCan.subscribeID
              << " sess=" << this;
    return;
  }
  trackIt->second->cancel(shared_from_this());
  auto res = writeFetchCancel(controlWriteBuf_, std::move(fetchCan));
  if (!res) {
    XLOG(ERR) << "writeFetchCancel failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (!setupComplete_) {
    XLOG(ERR) << "Uni stream before setup complete sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  // maybe not STREAM_HEADER_SUBGROUP, but at least not control
  co_withCancellation(
      cancellationSource_.getToken(),
      unidirectionalReadLoop(shared_from_this(), rh))
      .scheduleOn(evb_)
      .start();
}

void MoQSession::onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // TODO: prevent second control stream?
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    XLOG(ERR) << "Received bidi stream on client, kill it sess=" << this;
    bh.writeHandle->resetStream(/*error=*/0);
    bh.readHandle->stopSending(/*error=*/0);
  } else {
    bh.writeHandle->setPriority(0, 0, false);
    co_withCancellation(
        cancellationSource_.getToken(), controlReadLoop(bh.readHandle))
        .scheduleOn(evb_)
        .start();
    auto mergeToken = folly::CancellationToken::merge(
        cancellationSource_.getToken(), bh.writeHandle->getCancelToken());
    folly::coro::co_withCancellation(
        std::move(mergeToken), controlWriteLoop(bh.writeHandle))
        .scheduleOn(evb_)
        .start();
  }
}

void MoQSession::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  readBuf.append(std::move(datagram));
  folly::io::Cursor cursor(readBuf.front());
  auto type = quic::decodeQuicInteger(cursor);
  if (!type || StreamType(type->first) != StreamType::OBJECT_DATAGRAM) {
    XLOG(ERR) << __func__ << " Bad datagram header";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto dgLength = readBuf.chainLength();
  auto res = parseObjectHeader(cursor, dgLength);
  if (res.hasError()) {
    XLOG(ERR) << __func__ << " Bad Datagram: Failed to parse object header";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto remainingLength = cursor.totalLength();
  if (remainingLength != *res->length) {
    XLOG(ERR) << __func__ << " Bad datagram: Length mismatch";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  readBuf.trimStart(dgLength - remainingLength);
  auto alias = std::get_if<TrackAlias>(&res->trackIdentifier);
  XCHECK(alias);
  auto state = getSubscribeTrackReceiveState(*alias).get();
  if (state) {
    auto callback = state->getSubscribeCallback();
    if (callback) {
      callback->datagram(std::move(*res), readBuf.move());
    }
  }
}

bool MoQSession::closeSessionIfSubscribeIdInvalid(SubscribeID subscribeID) {
  if (maxSubscribeID_ <= subscribeID.value) {
    XLOG(ERR) << "Invalid subscribeID: " << subscribeID << " sess=" << this;
    close(SessionCloseErrorCode::TOO_MANY_SUBSCRIBES);
    return true;
  }
  return false;
}

/*static*/
uint64_t MoQSession::getMaxSubscribeIdIfPresent(
    const std::vector<SetupParameter>& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID)) {
      return param.asUint64;
    }
  }
  return 0;
}
} // namespace moxygen
