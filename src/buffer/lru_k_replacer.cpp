//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    printf("====\nhistory_list: ");
    for(auto it=history_list.begin(); it!=history_list.end(); it++) {
        printf("%d ", it->first);
    }
    printf("\naccess_list: ");
    for(auto it=access_list.begin(); it!=access_list.end(); it++) {
        printf("%d ", *it);
    }
    printf("\n====\n");
    // std::scoped_lock<std::mutex> lock(latch_);
    if(curr_size_ == 0 || history_list.empty()) {
        printf("Evict fail\n");
        return false;
    }
    
    // frame_id_t minI = history_list.end()->first;
    

    for(auto it=history_list.begin(); it!=history_list.end(); it++) {
        if(evictable_map[it->first]) {
            //删除
            evictable_map.erase(it->first);
            curr_size_--;
            *frame_id = it->first;
            history_list.erase(it);
            printf("Evict success frame_id: %d\n", *frame_id);
            return true;
        }
    }
    printf("Evict fail");
    return false;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id<=(int)replacer_size_, "frame_id is out of range");

    printf("====\nhistory_list: ");
    for(auto it=history_list.begin(); it!=history_list.end(); it++) {
        printf("%d ", it->first);
    }
    printf("\naccess_list: ");
    for(auto it=access_list.begin(); it!=access_list.end(); it++) {
        printf("%d ", *it);
    }
    printf("\n====\n");

    if(evictable_map.find(frame_id) == evictable_map.end()) {
        printf("new in frame_id: %d\n", frame_id);
        // 新加入的frame。
        if(curr_size_ == replacer_size_) {
            printf("RecordAccess full. evict\n");
            Evict(&frame_id);
        }
        SetEvictable(frame_id, true);

        if(history_list.size() != 0) {
            for(auto it2=history_list.rbegin(); it2!=history_list.rend(); it2++) {
                //如果遇到==k的,就跳过
                if(it2->second == k_) {
                    continue;
                } else {
                    //插入到it2的后面
                    history_list.insert(it2.base(), std::make_pair(frame_id, 1));
                    // history_pos_rec[frame_id] = history_list.end();
                    break;
                }
            }
        } else {
            history_list.push_back(std::make_pair(frame_id, 1));
            // history_pos_rec[frame_id] = history_list.end();
        }

        

    } else {
        printf("not new in frame_id: %d\n", frame_id);
        // frame_id_t pos = history_pos_rec[frame_id];
        // 查询history_list中对应的是否达到k, 如果达到k, 则将其evictable_map中的值设为false
        for(auto it=history_list.begin(); it!=history_list.end(); it++) {
            if(it->first == frame_id) {
                // if(it->second == k_) {
                //     history_list.erase(it); 
                //     if(access_list.size() == k_) {
                //         access_list.pop_front();
                //     }
                //     access_list.push_back(frame_id);
                //     SetEvictable(frame_id, false);
                // } else {


                // }
                for(auto it2=history_list.rbegin(); it2!=history_list.rend(); it2++) {
                    //如果遇到==k的,就跳过
                    if(it2->second == k_ && it->second+1 < k_) {
                        continue;
                    } else {
                        history_list.insert(it2.base(), std::make_pair(frame_id, it->second + 1));
                        printf("|-> frame_id: %d, it->second+1: %ld\n", frame_id, it->second+1);
                        history_list.erase(it);
                        // history_pos_rec[frame_id] = history_list.end();
                        break;
                    }
                }
                break;
            }
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id<=(int)replacer_size_, "frame_id is out of range");

    printf("====\nhistory_list: ");
    for(auto it=history_list.begin(); it!=history_list.end(); it++) {
        printf("%d ", it->first);
    }
    printf("\naccess_list: ");
    for(auto it=access_list.begin(); it!=access_list.end(); it++) {
        printf("%d ", *it);
    }
    printf("\n====\n");

    if(!evictable_map[frame_id] && set_evictable) {
        evictable_map[frame_id] = true;
        curr_size_++;

        printf("SetEvictable frame_id: %d. now curr_size_: %ld\n", frame_id, curr_size_);
    } else if (evictable_map[frame_id] && !set_evictable) {

        evictable_map[frame_id] = false;
        curr_size_--;
        printf("SetUnEvictable frame_id: %d. now curr_size_: %ld\n", frame_id, curr_size_);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    // std::scoped_lock<std::mutex> lock(latch_);
    BUSTUB_ASSERT(frame_id<=(int)replacer_size_, "frame_id is out of range");
    BUSTUB_ASSERT(!evictable_map[frame_id], "frame_id is not evictable");
    if(evictable_map.find(frame_id) == evictable_map.end()) {
        return;
    }
    if(history_list.size() == 0) {
        return;
    }
    evictable_map.erase(frame_id);
    std::list<std::pair<frame_id_t, size_t>>::iterator it = history_list.begin();
    for(; it!=history_list.end(); it++) {
        if(it->first == frame_id) {
            history_list.erase(it);
            break;
        }
    }
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {

    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_;
}

}  // namespace bustub
