/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>

namespace moxygen::test {

inline folly::StringPiece getContainingDirectory(folly::StringPiece input) {
  auto pos = folly::rfind(input, '/');
  if (pos == std::string::npos) {
    pos = 0;
  } else {
    pos += 1;
  }
  return input.subpiece(0, pos);
}

// Test video generated by:
// ffmpeg -y -f lavfi -i smptebars=duration=1:size=320x200:rate=30 -f lavfi -re
// -i sine=frequency=1000:duration=1:sample_rate=48000 -pix_fmt yuv420p -c:v
// libx264 -b:v 180k -g 60 -keyint_min 60 -profile:v baseline -preset veryfast
// -c:a libfdk_aac -b:a 96k -vf
// "drawtext=fontfile=/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf:
// text=\'Local time %{localtime\: %Y\/%m\/%d %H.%M.%S} (%{n})\': x=10: y=10:
// fontsize=16: fontcolor=white: box=1: boxcolor=0x00000099" -f flv
// ~/test_files/testOK1s.flv

} // namespace moxygen::test
