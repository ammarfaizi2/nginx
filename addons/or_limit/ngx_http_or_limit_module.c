/*
 * Copyright (C) 2026 OpenResty Inc.
 * Author: Ammar Faizi <ammarfaizi2@openresty.com>
 */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <stdbool.h>

#define ONE_TOKEN_MT 1000000ULL

/* Shared memory node. */
typedef struct {
    ngx_str_node_t      sn;            /* Must be first.      */
    ngx_queue_t         queue;         /* The LRU queue link. */
    uint16_t            iter_logged;
    uint32_t            nr_requests;
    uint64_t            last_logged;
    uint64_t            tokens_mt;
    uint64_t            last_update;
    uint64_t            blocked_until;
} ngx_http_or_limit_node_t;

/* Shared memory context. */
typedef struct {
    ngx_rbtree_t        rbtree;
    ngx_rbtree_node_t   sentinel;
    ngx_queue_t         queue;         /* The head of the LRU queue. */
    ngx_int_t           index;
    uint64_t            burst_mt;
    uint64_t            refill_tokens;
    uint64_t            refill_period_ms;
    uint64_t            penalty_ms;
    uint64_t            max_idle_ms;
} ngx_http_or_limit_shctx_t;

/* Module configuration structs. */
typedef struct {
    ngx_shm_zone_t     *shm_zone;
} ngx_http_or_limit_loc_conf_t;

static char *ngx_http_or_limit_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_or_limit_req(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

/* Configuration directives. */
static ngx_command_t ngx_http_or_limit_commands[] = {
    /*
     * Define the zone: or_limit_zone <key> <name> mem=64m burst=60 fill_per_sec=2 penalty=30
     */
    { ngx_string("or_limit_zone"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE6,
      ngx_http_or_limit_zone,
      0, 0, NULL },

    /*
     * Apply to location: or_limit_req zone_name.
     */
    { ngx_string("or_limit_req"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_or_limit_req,
      NGX_HTTP_LOC_CONF_OFFSET,
      0, NULL },

      ngx_null_command
};

static ngx_int_t ngx_http_or_limit_init(ngx_conf_t *cf);
static void *ngx_http_or_limit_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_or_limit_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

/* Module context definition. */
static ngx_http_module_t ngx_http_or_limit_module_ctx = {
    NULL,                              /* Preconfiguration.                  */
    ngx_http_or_limit_init,            /* Postconfiguration (Hooks handler). */
    NULL,                              /* Create main configuration.         */
    NULL,                              /* Init main configuration.           */
    NULL,                              /* Create server configuration.       */
    NULL,                              /* Merge server configuration.        */
    ngx_http_or_limit_create_loc_conf, /* Create location configuration.     */
    ngx_http_or_limit_merge_loc_conf   /* Merge location configuration.      */
};

/* Module definition. */
extern ngx_module_t ngx_http_or_limit_module;

ngx_module_t ngx_http_or_limit_module = {
    NGX_MODULE_V1,
    &ngx_http_or_limit_module_ctx,     /* Module context.    */
    ngx_http_or_limit_commands,        /* Module directives. */
    NGX_HTTP_MODULE,                   /* Module type.       */
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_int_t ngx_http_or_limit_handler(ngx_http_request_t *r);

/* Hook the handler into NGINX request processing. */
static ngx_int_t
ngx_http_or_limit_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    h = ngx_array_push(&cmcf->phases[NGX_HTTP_PREACCESS_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_or_limit_handler;
    return NGX_OK;
}

static void
ngx_http_or_limit_expire(ngx_http_or_limit_shctx_t *ctx,
                         ngx_slab_pool_t *shpool, ngx_msec_t now)
{
    ngx_http_or_limit_node_t  *node;
    ngx_msec_t                 elapsed_ms;
    ngx_queue_t               *q;
    int                        i;

    /*
     * Check the oldest 2 nodes on every request to keep
     * memory clean without burning CPU.
     */
    for (i = 0; i < 2; i++) {

        if (ngx_queue_empty(&ctx->queue)) {
            return;
        }

        /*
         * Get the tail (oldest element).
         */
        q = ngx_queue_last(&ctx->queue);

        /*
         * ngx_queue_data is a magical NGINX macro that gets
         * the parent struct from the queue pointer.
         */
        node = ngx_queue_data(q, ngx_http_or_limit_node_t, queue);
        elapsed_ms = now - node->last_update;

        /*
         * Only evict if the bucket is fully refilled AND not
         * serving a penalty.
         */
        if (elapsed_ms >= ctx->max_idle_ms && now >= node->blocked_until) {
            ngx_queue_remove(q);
            ngx_rbtree_delete(&ctx->rbtree, &node->sn.node);
            ngx_slab_free_locked(shpool, node);
        } else {
            /*
             * If the absolute oldest node is still active,
             * everything else is newer. Stop looking.
             */
            return;
        }
    }
}

static bool
extract_ip_addr(ngx_http_variable_value_t *in, u_char out[16],
                size_t *out_len)
{
    u_char str_tmp[INET6_ADDRSTRLEN];

    if (in->len > (sizeof(str_tmp)-1)) {
        return false;
    }

    ngx_memcpy(str_tmp, in->data, in->len);
    str_tmp[in->len] = '\0';

    /*
     * Use INET_ADDRSTRLEN (16) and INET6_ADDRSTRLEN (46)
     * for safer length boundaries.
     */
    if (in->len >= 7 && in->len <= 15 &&
        inet_pton(AF_INET, (char *) str_tmp, out) == 1) {
        *out_len = 4;
        return true;
    }

    if (in->len >= 3 && in->len <= 45 &&
        inet_pton(AF_INET6, (char *) str_tmp, out) == 1) {
        if (IN6_IS_ADDR_V4MAPPED((struct in6_addr *) out)) {
            /*
             * If it's IPv4-mapped IPv6 address, extract
             * the IPv4 part.
             */
            ngx_memmove(out, out + 12, 4);
            *out_len = 4;
        } else {
            *out_len = 16;
        }
        return true;
    }

    return false;
}

static void
stringify_key(char key[512], ngx_http_or_limit_node_t *node)
{
    void *data = node->sn.str.data;
    size_t len = node->sn.str.len;

    /*
     * If the key looks like an IP address in binary form, convert
     * it to human-readable string for better log messages. Otherwise,
     * fallback to hex string.
     *
     * Note: We use the length of the original variable value as a
     * hint to determine if it's IPv4 or IPv6. This is not 100%
     * accurate but should work well in practice since most keys are
     * either raw IPs or strings.
     */
    if (len == 4) {
        if (inet_ntop(AF_INET, node->sn.str.data, key, 16)) {
            return;
        }
    } else if (len == 16) {
        if (inet_ntop(AF_INET6, node->sn.str.data, key, 46)) {
            return;
        }
    }

    /*
     * Fallback to hex string if it's not a valid IP.
     */
    ngx_hex_dump((u_char *) key, data, len > 256 ? 256 : len);
}

static void
log_rate_limited_req(ngx_http_request_t *r, ngx_http_or_limit_node_t *node,
                     uint64_t blocked_ms)
{
    uint64_t now = ngx_current_msec;
    const char *suppress_msg = "";
    char key[128];

    if (node->iter_logged >= 5) {
        /*
         * Skip logging if we've already logged 5 times
         * for this node to avoid log flooding.
         */
        return;
    }

    if (now - node->last_logged < 60000) {
        /*
         * Skip logging if the last log for this node was
         * less than 60 seconds ago to avoid log flooding.
         */
        return;
    }

    if (++node->iter_logged >= 5) {
        suppress_msg = " (further logs for this node will be suppressed)";
    }

    stringify_key(key, node);
    node->last_logged = now;
    ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0,
                  "or_limit: rate limited, blocked for %d ms [iter=%d, nr_req=%d, key=%s]%s",
                  (int)blocked_ms, (int)node->iter_logged, (int)node->nr_requests,
                  key, suppress_msg);
}

/* The core rate limit logic. */
static ngx_int_t
ngx_http_or_limit_handler(ngx_http_request_t *r)
{
    ngx_http_or_limit_loc_conf_t  *llcf;
    ngx_http_or_limit_shctx_t     *ctx;
    ngx_slab_pool_t               *shpool;
    ngx_rbtree_node_t             *node, *sentinel;
    ngx_http_variable_value_t     *vv;
    u_char                         out_ip[64];
    ngx_str_t                      key;
    uint32_t                       hash;
    ngx_msec_t                     now, elapsed_ms;
    ngx_http_or_limit_node_t      *cur_node;
    ngx_int_t                      rc = NGX_DECLINED;
    size_t                         alloc_sz;

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_or_limit_module);
    if (llcf->shm_zone == NULL) {
        /* Not enabled here. */
        return NGX_DECLINED;
    }

    ctx = llcf->shm_zone->data;
    shpool = (ngx_slab_pool_t *) llcf->shm_zone->shm.addr;

    /*
     * Fetch the variable using the pre-compiled index.
     *
     * This is expected to contain the client IP address
     * or any other unique identifier for rate limiting.
     */
    vv = ngx_http_get_indexed_variable(r, ctx->index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        /*
         * If the variable doesn't exist or is empty,
         * skip the rate limiting.
         */
        return NGX_DECLINED;
    }

    /*
     * Try to extract the IP address in binary form. If
     * it fails, use the raw variable value as the key.
     */
    key.len = 0;
    if (extract_ip_addr(vv, out_ip, &key.len)) {
        key.data = out_ip;
        if (key.len == 4) {
            /*
             * Fast-path for IPv4 (Exactly 4 bytes = 32 bits).
             *
             * Use the raw IP as a perfect, collision-free
             * 32-bit hash.
             */
            memcpy(&hash, key.data, 4);
        } else {
            hash = ngx_crc32_short(key.data, key.len);
        }
    } else {
        key.len = vv->len;
        key.data = vv->data;
        hash = ngx_crc32_short(key.data, key.len);
    }

    now = ngx_current_msec;
    ngx_shmtx_lock(&shpool->mutex);

    /*
     * Run periodic cleanup.
     */
    ngx_http_or_limit_expire(ctx, shpool, now);

    node = ctx->rbtree.root;
    sentinel = ctx->rbtree.sentinel;

    /* Search RB-tree. */
    while (node != sentinel) {
        if (hash < node->key) {
            node = node->left;
            continue;
        }

        if (hash > node->key) {
            node = node->right;
            continue;
        }

        cur_node = (ngx_http_or_limit_node_t *) node;

        if (key.len == cur_node->sn.str.len &&
            ngx_memcmp(key.data, cur_node->sn.str.data, key.len) == 0) {
            goto found;
        }

        if (ngx_memcmp(key.data, cur_node->sn.str.data, key.len) < 0) {
            node = node->left;
        } else {
            node = node->right;
        }
    }

    /*
     * The key is not found, create it!
     */
    alloc_sz = sizeof(ngx_http_or_limit_node_t) + key.len;
    cur_node = ngx_slab_alloc_locked(shpool, alloc_sz);
    if (cur_node == NULL) {
        /*
         * We are out of memory!
         * Force evict the oldest node even if it's active.
         */
        if (!ngx_queue_empty(&ctx->queue)) {
            ngx_http_or_limit_node_t *old_node;
            ngx_queue_t *q;

            q = ngx_queue_last(&ctx->queue);
            old_node = ngx_queue_data(q, ngx_http_or_limit_node_t, queue);

            ngx_queue_remove(q);
            ngx_rbtree_delete(&ctx->rbtree, &old_node->sn.node);
            ngx_slab_free_locked(shpool, old_node);

            /*
             * Try allocating one more time after the
             * forced eviction.
             */
            cur_node = ngx_slab_alloc_locked(shpool, alloc_sz);
        }

        if (cur_node == NULL) {
            /*
             * Still out of memory after eviction. Give up.
             */
            ngx_shmtx_unlock(&shpool->mutex);
            ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0,
                          "or_limit: failed to allocate memory for new node");

            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
    }

    cur_node->sn.node.key = hash;
    cur_node->sn.str.len = key.len;
    cur_node->sn.str.data = (u_char *) cur_node + sizeof(ngx_http_or_limit_node_t);
    ngx_memcpy(cur_node->sn.str.data, key.data, key.len);

    if (ctx->burst_mt >= ONE_TOKEN_MT) {
        cur_node->tokens_mt = ctx->burst_mt - ONE_TOKEN_MT;
    } else {
        cur_node->tokens_mt = 0;
    }
    cur_node->last_update = now;
    cur_node->blocked_until = 0;
    cur_node->iter_logged = 0;
    cur_node->last_logged = 0;
    cur_node->nr_requests = 1;
    ngx_rbtree_insert(&ctx->rbtree, &cur_node->sn.node);

    /*
     * Add the new node to head of LRU queue.
     */
    ngx_queue_insert_head(&ctx->queue, &cur_node->queue);
    rc = NGX_DECLINED;
    goto done;

found:
    cur_node->nr_requests++;

    /*
     * Move the active node to the head (front)
     * of the LRU queue.
     */
    ngx_queue_remove(&cur_node->queue);
    ngx_queue_insert_head(&ctx->queue, &cur_node->queue);

    /*
     * Check whether the node is currently blocked.
     */
    if (cur_node->blocked_until > 0 && now < cur_node->blocked_until) {
        uint64_t blocked_ms;

        rc = NGX_HTTP_TOO_MANY_REQUESTS;
        r->keepalive = 0;

        blocked_ms = cur_node->blocked_until - now;

        /*
         * Two levels of penalty:
         * 1) If the client is blocked less than 2x the configured
         *    penalty time, add a small penalty of 200ms.
         *
         * 2) If the client is blocked more than 2x the configured
         *    penalty time, add a larger penalty of 500ms and shut
         *    down the connection immediately (no response will be
         *    sent for this request at all).
         */
        if (blocked_ms >= (ctx->penalty_ms * 2)) {
            ngx_shutdown_socket(r->connection->fd, NGX_RDWR_SHUTDOWN);
            cur_node->blocked_until += 500;
        } else {
            cur_node->blocked_until += 200;
        }
        log_rate_limited_req(r, cur_node, blocked_ms);
        goto done;
    }

    /*
     * The client is not currently blocked. Fill the
     * bucket with tokens based on the elapsed time
     * since the last update.
     */
    elapsed_ms = now - cur_node->last_update;
    if (elapsed_ms > ctx->max_idle_ms) {
        cur_node->tokens_mt = ctx->burst_mt;
    } else if (elapsed_ms > 0) {
        uint64_t added_mt;

        added_mt = (elapsed_ms * ctx->refill_tokens * ONE_TOKEN_MT) / ctx->refill_period_ms;
        cur_node->tokens_mt += added_mt;
        if (cur_node->tokens_mt > ctx->burst_mt) {
            cur_node->tokens_mt = ctx->burst_mt;
        }
    }

    cur_node->last_update = now;
    cur_node->blocked_until = 0;
    cur_node->iter_logged = 0;
    cur_node->last_logged = 0;

    /*
     * Consume a token and penalize if the client
     * runs out of tokens.
     */
    if (cur_node->tokens_mt >= ONE_TOKEN_MT) {
        /*
         * Request is allowed. Consume one token.
         */
        cur_node->tokens_mt -= ONE_TOKEN_MT;
        rc = NGX_DECLINED;
    } else {
        /*
         * The client has run out of tokens.
         * Block it and return 429 Too Many Requests.
         */
        cur_node->tokens_mt = 0;
        cur_node->blocked_until = now + ctx->penalty_ms;
        log_rate_limited_req(r, cur_node, ctx->penalty_ms);

        rc = NGX_HTTP_TOO_MANY_REQUESTS;
        r->keepalive = 0;
    }

done:
    ngx_shmtx_unlock(&shpool->mutex);
    return rc;
}

/*
 * Initialize the shared memory zone.
 */
static ngx_int_t
ngx_http_or_limit_init_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    ngx_http_or_limit_shctx_t  *ctx, *octx = data;
    ngx_slab_pool_t            *shpool;
    ngx_http_or_limit_shctx_t  *cfg_ctx;

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    if (octx) {
        shm_zone->data = octx;
        return NGX_OK;
    }

    ctx = ngx_slab_alloc(shpool, sizeof(ngx_http_or_limit_shctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ngx_rbtree_init(&ctx->rbtree, &ctx->sentinel, ngx_str_rbtree_insert_value);

    /*
     * Initialize the LRU doubly-linked list.
     */
    ngx_queue_init(&ctx->queue);

    /*
     * Copy config struct from shm_zone to our shared context.
     */
    cfg_ctx = shm_zone->data;
    ctx->burst_mt = cfg_ctx->burst_mt;
    ctx->refill_tokens = cfg_ctx->refill_tokens;
    ctx->refill_period_ms = cfg_ctx->refill_period_ms;
    ctx->penalty_ms = cfg_ctx->penalty_ms;
    ctx->max_idle_ms = cfg_ctx->max_idle_ms;
    ctx->index = cfg_ctx->index;

    shm_zone->data = ctx;
    return NGX_OK;
}

/*
 * Parse: or_limit_zone <key> <name> mem=64m burst=60 fill_per_sec=2 penalty=30
 */
static char *
ngx_http_or_limit_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                  *value, name;
    ngx_shm_zone_t             *shm_zone;
    ngx_http_or_limit_shctx_t  *ctx;
    ngx_int_t                   size = -1, burst = -1, rate = -1, penalty = -1, index;
    bool                        mem_set, burst_set, fill_set, penalty_set;
    size_t                      i, idx;

    value = cf->args->elts;

    /*
     * Extract and compile the variable (e.g., $http_cf_connecting_ip).
     */
    if (value[1].data[0] != '$') {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid variable name \"%V\"", &value[1]);
        return NGX_CONF_ERROR;
    }

    name.len = value[1].len - 1; /* Strip the '$'. */
    name.data = value[1].data + 1;

    index = ngx_http_get_variable_index(cf, &name);
    if (index == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }

    /*
     * Parse the rest of the arguments (shifted by 1).
     */
    mem_set = burst_set = fill_set = penalty_set = false;
    for (i = 0; i < 4; i++) {
        /*
         * Shift by 3 to account for the variable name.
         */
        idx = i + 3;

        if (!ngx_strncmp(value[idx].data, "mem=", 4)) {
            ngx_str_t mem_str = { value[idx].len - 4, value[idx].data + 4 };
            size = ngx_parse_size(&mem_str);
            if (size == NGX_ERROR) {
                return "invalid mem= size";
            }

            mem_set = true;
            continue;
        }

        if (!ngx_strncmp(value[idx].data, "burst=", 6)) {
            ngx_str_t burst_str = { value[idx].len - 6, value[idx].data + 6 };
            burst = ngx_atoi(burst_str.data, burst_str.len);
            if (burst == NGX_ERROR || burst < 0) {
                return "invalid burst size";
            }

            burst_set = true;
            continue;
        }

        if (!ngx_strncmp(value[idx].data, "fill_per_sec=", 13)) {
            ngx_str_t rate_str = { value[idx].len - 13, value[idx].data + 13 };
            rate = ngx_atoi(rate_str.data, rate_str.len);
            if (rate == NGX_ERROR || rate < 0) {
                return "invalid fill_per_sec rate";
            }

            fill_set = true;
            continue;
        }

        if (!ngx_strncmp(value[idx].data, "penalty=", 8)) {
            ngx_str_t penalty_str = { value[idx].len - 8, value[idx].data + 8 };
            penalty = ngx_atoi(penalty_str.data, penalty_str.len);
            if (penalty == NGX_ERROR || penalty < 0) {
                return "invalid penalty";
            }

            penalty_set = true;
            continue;
        }

        return "invalid argument";
    }

    if (!mem_set) {
        return "missing mem= argument";
    }

    if (!burst_set) {
        return "missing burst= argument";
    }

    if (!fill_set) {
        return "missing fill_per_sec= argument";
    }

    if (!penalty_set) {
        return "missing penalty= argument";
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_or_limit_shctx_t));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->index = index; /* Store the compiled variable index */
    ctx->burst_mt = burst * ONE_TOKEN_MT;
    ctx->refill_tokens = rate;
    ctx->refill_period_ms = 1000;
    ctx->penalty_ms = penalty * 1000;
    ctx->max_idle_ms = (burst * 1000) / rate;

    shm_zone = ngx_shared_memory_add(cf, &value[2], size,
                                     &ngx_http_or_limit_module);
    if (shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    shm_zone->init = ngx_http_or_limit_init_zone;
    shm_zone->data = ctx;

    return NGX_CONF_OK;
}

/*
 * Parse: or_limit_req zone_name
 */
static char *
ngx_http_or_limit_req(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_or_limit_loc_conf_t *llcf = conf;
    ngx_str_t *value = cf->args->elts;

    if (llcf->shm_zone != NULL) {
        return "is duplicate";
    }

    llcf->shm_zone = ngx_shared_memory_add(cf, &value[1], 0,
                                           &ngx_http_or_limit_module);
    if (llcf->shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

/* Location config allocation. */
static void *
ngx_http_or_limit_create_loc_conf(ngx_conf_t *cf)
{
    return ngx_pcalloc(cf->pool, sizeof(ngx_http_or_limit_loc_conf_t));
}

static char *
ngx_http_or_limit_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_or_limit_loc_conf_t *prev = parent;
    ngx_http_or_limit_loc_conf_t *conf = child;
    if (conf->shm_zone == NULL) {
        conf->shm_zone = prev->shm_zone;
    }
    return NGX_CONF_OK;
}
