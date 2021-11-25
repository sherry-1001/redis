/* KVDK module -- A few examples of the Redis Modules API in the form
 * of commands showing how to accomplish common tasks.
 *
 * This module does not do anything useful, if not for a few commands. The
 * examples are designed in order to show the API.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be
 used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "../../deps/kvdk/include/kvdk/engine.h"
#include "../redismodule.h"
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define PMEM_FILE_SIZE 256ULL << 30
#define HASH_BUCKET_NUM 1 << 27
#define PMEM_SEG_BLOCKS 2 * 1024 * 1024
#define PMEM_BLOCK_SIZE 64
#define HASH_BUCKET_SIZE 128
#define NUM_BUCKETS_PER_SLOT 1
#define POPULATE_PMEM_SPACE 1U

const char *engine_path = "";
KVDKEngine *engine;
KVDKConfigs *config = NULL;

static const char *enum_to_str[] = {FOREACH_ENUM(GENERATE_STRING)};

int GetInt64Value(uint64_t *var, const char *value) {
  if (strstr(value, "<<")) {
    uint64_t left_val, right_val;
    left_val = strtoull(strtok((char *)value, "<<"), NULL, 10);
    right_val = strtoull(strtok(NULL, "<<"), NULL, 10);
    *var = left_val << right_val;
  } else if (strstr(value, ">>")) {
    uint64_t left_val, right_val;
    left_val = strtoull(strtok((char *)value, ">>"), NULL, 10);
    right_val = strtoull(strtok(NULL, ">>"), NULL, 10);
    *var = left_val >> right_val;
  } else if (strstr(value, "*")) {
    *var = 1;
    char *p = strtok((char *)value, "*");
    while (p) {
      (*var) *= strtoull(p, NULL, 10);
      p = strtok(NULL, "*");
    }
  } else {
    *var = strtoull(value, NULL, 10);
  }
  return 1;
}

int GetInt32Value(uint32_t *var, const char *value) {
  if (strstr(value, "<<")) {
    uint32_t left_val, right_val;
    left_val = (uint32_t)strtoul(strtok((char *)value, "<<"), NULL, 10);
    right_val = (uint32_t)strtoul(strtok(NULL, "<<"), NULL, 10);
    *var = left_val << right_val;
  } else if (strstr(value, ">>")) {
    uint32_t left_val, right_val;
    left_val = (uint32_t)strtoul(strtok((char *)value, ">>"), NULL, 10);
    right_val = (uint32_t)strtoul(strtok(NULL, ">>"), NULL, 10);
    *var = left_val >> right_val;
  } else if (strstr(value, "*")) {
    *var = 1;
    char *p = strtok((char *)value, "*");
    while (p) {
      (*var) *= (uint32_t)strtoul(p, NULL, 10);
      p = strtok(NULL, "*");
    }
  } else {
    *var = strtoull(value, NULL, 10);
  }
  return 1;
}

KVDKConfigs *LoadAndCreateConfigs(RedisModuleString **argv, int argc) {
  uint64_t pmem_file_size = PMEM_FILE_SIZE, hash_bucket_num = HASH_BUCKET_NUM,
           pmem_segment_blocks = PMEM_SEG_BLOCKS, max_write_threads;
  uint32_t pmem_block_size = PMEM_BLOCK_SIZE,
           hash_bucket_size = HASH_BUCKET_SIZE,
           num_buckets_per_slot = NUM_BUCKETS_PER_SLOT;
  unsigned char populate_pmem_space = POPULATE_PMEM_SPACE;
  /* Log the list of parameters passing loading the module. */
  for (int j = 0; j < argc; j += 2) {
    const char *config_name = RedisModule_StringPtrLen(argv[j], NULL);
    const char *config_value = RedisModule_StringPtrLen(argv[j + 1], NULL);
    printf("Module loaded with ARG_NAME[%d] = %s, ARG_VALUE[%d] = %s\n", j,
           config_name, j + 1, config_value);
    if ((!strcmp(config_name, "pmem_file_size") &&
         GetInt64Value(&pmem_file_size, config_value)) ||
        (!strcmp(config_name, "pmem_segment_blocks") &&
         GetInt64Value(&pmem_segment_blocks, config_value)) ||
        (!strcmp(config_name, "hash_bucket_num") &&
         GetInt64Value(&hash_bucket_num, config_value)) ||
        (!strcmp(config_name, "max_write_threads") &&
         GetInt64Value(&max_write_threads, config_value))) {
      continue;
    } else if (!strcmp(config_name, "populate_pmem_space")) {
      populate_pmem_space = (unsigned char)atoi(config_value);
    } else if ((!strcmp(config_name, "pmem_block_size") &&
                GetInt32Value(&pmem_block_size, config_value)) ||
               (!strcmp(config_name, "hash_bucket_size") &&
                GetInt32Value(&hash_bucket_size, config_value)) ||
               (!strcmp(config_name, "num_buckets_per_slot") &&
                GetInt32Value(&num_buckets_per_slot, config_value))) {
      continue;
    } else if (!strcmp(config_name, "engine_path")) {
      engine_path = config_value;
    } else {
      assert(0 && "not support this config");
    }
  }

  KVDKConfigs *kvdk_configs = KVDKCreateConfigs();
  KVDKUserConfigs(kvdk_configs, max_write_threads, pmem_file_size,
                  populate_pmem_space, pmem_block_size, pmem_segment_blocks,
                  hash_bucket_size, hash_bucket_num, num_buckets_per_slot);
  return kvdk_configs;
}

int InitEngine(RedisModuleString **argv, int argc) {
  config = LoadAndCreateConfigs(argv, argc);
  if (config == NULL) {
    return REDISMODULE_ERR;
  }
  if ((engine_path != NULL && engine_path[0] == '\0')) {
    return REDISMODULE_ERR;
  }

  // Purge old KVDK instance
  KVDKRemovePMemContents(engine_path);

  // open engine
  KVDKStatus s = KVDKOpen(engine_path, config, stdout, &engine);

  if (s != Ok) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int KVDKSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  const char *val_str = RedisModule_StringPtrLen(argv[2], &val_len);

  KVDKStatus s = KVDKSet(engine, key_str, key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);
  size_t val_len;
  char *val_str;
  KVDKStatus s = KVDKGet(engine, key_str, key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
}

int KVDKDelete_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[1], &key_len);

  KVDKStatus s = KVDKDelete(engine, key_str, key_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKHSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 4)
    return RedisModule_WrongArity(ctx);
  size_t collection_len, key_len, val_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);
  const char *val_str = RedisModule_StringPtrLen(argv[3], &val_len);

  KVDKStatus s = KVDKHashSet(engine, collection_str, collection_len, key_str,
                             key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKHGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t collection_len, key_len, val_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);
  char *val_str;
  KVDKStatus s = KVDKHashGet(engine, collection_str, collection_len, key_str,
                             key_len, &val_len, &val_str);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithStringBuffer(ctx, val_str, val_len);
  ;
}

int KVDKHDelete_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t collection_len, key_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);

  KVDKStatus s =
      KVDKHashDelete(engine, collection_str, collection_len, key_str, key_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKSAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t set_len, key_len;
  const char *set_str = RedisModule_StringPtrLen(argv[1], &set_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);

  KVDKStatus s = KVDKHashSet(engine, set_str, set_len, key_str, key_len, "", 0);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKSRemove_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t set_len, key_len;
  const char *set_str = RedisModule_StringPtrLen(argv[1], &set_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);

  KVDKStatus s = KVDKHashDelete(engine, set_str, set_len, key_str, key_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int value_compare_func(const char *src_value, size_t src_val_len,
                       const char *target_value, size_t target_val_len) {
  double src_score = strtod(src_value, NULL);
  double target_score = strtod(target_value, NULL);
  if (src_score == target_score)
    return 0;
  else if (src_score < target_score)
    return -1;
  else
    return 1;
}

int KVDKZAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 4)
    return RedisModule_WrongArity(ctx);
  size_t collection_len, key_len, val_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);
  const char *val_str = RedisModule_StringPtrLen(argv[3], &val_len);

  // KVDKSetSortedCompareFunc(engine, collection_str, collection_len, NULL,
  // value_compare_func); KVDKStatus s = KVDKSortedDelete(engine,
  // collection_str, collection_len,
  //                                 key_str, key_len);
  KVDKStatus s = KVDKSortedSet(engine, collection_str, collection_len, key_str,
                               key_len, val_str, val_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKLPUSH_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc) {
  printf("arg num: %d\n", argc);
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t collection_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);

  KVDKStatus s =
      KVDKLPush(engine, collection_str, collection_len, key_str, key_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKLPOP_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t collection_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);

  char *key_str;
  size_t key_len;
  KVDKStatus s =
      KVDKLPop(engine, collection_str, collection_len, &key_str, &key_len);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithStringBuffer(ctx, key_str, key_len);
}

int KVDKRPUSH_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc) {
  if (argc != 3)
    return RedisModule_WrongArity(ctx);
  size_t collection_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  size_t key_len;
  const char *key_str = RedisModule_StringPtrLen(argv[2], &key_len);

  KVDKStatus s =
      KVDKRPush(engine, collection_str, collection_len, key_str, key_len);
  if (s != Ok) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

int KVDKRPOP_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 2)
    return RedisModule_WrongArity(ctx);
  size_t collection_len;
  const char *collection_str =
      RedisModule_StringPtrLen(argv[1], &collection_len);
  char *key_str;
  size_t key_len;
  KVDKStatus s =
      KVDKRPop(engine, collection_str, collection_len, &key_str, &key_len);
  if (s != Ok && s != NotFound) {
    return RedisModule_ReplyWithError(ctx, enum_to_str[s]);
  }
  return RedisModule_ReplyWithStringBuffer(ctx, key_str, key_len);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  // must have `max_write_threads` and `engine_path`
  if (argc % 2 != 0 && (argc / 2) < 2 && (argc / 2) > 10) {
    return RedisModule_WrongArity(ctx);
  }
  if (RedisModule_Init(ctx, "kvdk", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (InitEngine(argv, argc) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // ========================== string ============================
  if (RedisModule_CreateCommand(ctx, "kvdk.set", KVDKSet_RedisCommand, "write",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.get", KVDKGet_RedisCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.delete", KVDKDelete_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // ========================== hash ================================
  if (RedisModule_CreateCommand(ctx, "kvdk.hset", KVDKHSet_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.hget", KVDKHGet_RedisCommand,
                                "readonly", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.hdelete", KVDKHDelete_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // ========================== set ==================================
  if (RedisModule_CreateCommand(ctx, "kvdk.sadd", KVDKSAdd_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.srem", KVDKSRemove_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // ========================== skiplist ==================================
  if (RedisModule_CreateCommand(ctx, "kvdk.zadd", KVDKZAdd_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // ========================== lists ==================================
  if (RedisModule_CreateCommand(ctx, "kvdk.lpush", KVDKLPUSH_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.lpop", KVDKLPOP_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.rpush", KVDKRPUSH_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "kvdk.rpop", KVDKRPOP_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
  REDISMODULE_NOT_USED(ctx);
  KVDKConfigsDestory(config);
  KVDKCloseEngine(engine);
  return REDISMODULE_OK;
}
