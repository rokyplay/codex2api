/**
 * codex2api 管理面板 — 工具函数
 * API 封装、HTML 转义、时间格式化、防抖
 */

/* ============ i18n 国际化 ============ */

var _i18n = {
  zh: {
    // 登录
    'login.title': 'codex2api',
    'login.subtitle': '管理面板',
    'login.username': '用户名',
    'login.password': '密码',
    'login.username_placeholder': '请输入用户名',
    'login.password_placeholder': '请输入密码',
    'login.submit': '登 录',
    'login.submitting': '登录中...',
    'login.error_empty': '请输入用户名和密码',
    'login.success': '登录成功',
    'login.failed': '登录失败',
    'login.expired': '认证已过期，请重新登录',
    'login.logged_out': '已退出登录',
    'login.mode': '登录模式',
    'login.mode_password_totp': '密码 + TOTP',
    'login.mode_totp_only': '仅 TOTP',
    'login.totp_code': 'TOTP 验证码',
    'login.totp_placeholder': '请输入 {digits} 位验证码',
    'login.totp_required': '请输入 TOTP 验证码',

    // 导航
    'nav.dashboard': '仪表盘',
    'nav.accounts': '账号管理',
    'nav.statistics': '统计',
    'nav.config': '配置',
    'nav.logs': '日志',
    'nav.registration': '注册',
    'nav.abuse': '风控',

    // 注册
    'registration.checking': '检查连接...',
    'registration.server_online': '注册服务器在线',
    'registration.server_offline': '注册服务器离线',
    'registration.count': '数量',
    'registration.concurrency': '并发',
    'registration.proxy': '代理',
    'registration.auto_upload': '自动上传',
    'registration.start': '开始注册',
    'registration.stop': '停止',
    'registration.progress': '注册进度',
    'registration.success': '成功',
    'registration.failed': '失败',
    'registration.uncertain': '不确定',
    'registration.total': '总计',
    'registration.with_token': '有Token',
    'registration.remote_accounts': '注册机账号',
    'registration.registered_at': '注册时间',
    'registration.no_accounts': '暂无注册记录',
    'registration.load_failed': '加载失败',
    'registration.confirm_start': '确认启动注册 {count} 个账号？',
    'registration.job_started': '注册任务已启动: {count} 个',
    'registration.start_failed': '启动失败',
    'registration.job_stopping': '正在停止任务...',
    'registration.stop_failed': '停止失败',
    'registration.job_completed': '注册任务已完成',
    'registration.invalid_count': '数量必须在 1-200 之间',
    'registration.monitor_title': '注册成功率监控',
    'registration.step_mail_create': '邮箱创建',
    'registration.step_oauth_init': 'OAuth 初始化',
    'registration.step_sentinel_reg': 'Sentinel 验证 #1',
    'registration.step_email_otp': '邮件验证',
    'registration.step_sentinel_create': 'Sentinel 验证 #2',
    'registration.step_oauth_callback': 'OAuth 回调',
    'registration.overall_rate': '总成功率',
    'registration.no_stats': '暂无统计数据',
    'registration.history_title': '历史任务',
    'registration.history_empty': '暂无历史记录',
    'registration.history_job_id': '任务 ID',
    'registration.history_time': '时间',
    'registration.history_count': '数量',
    'registration.history_result': '结果',
    'registration.history_elapsed': '用时',
    'registration.history_detail': '步骤统计',
    'pool_health.title': '池健康监控',
    'pool_health.probe_status': '探测状态',
    'pool_health.guard_status': '守护状态',
    'pool_health.registering': '补号中',
    'pool_health.idle': '空闲',

    // 统计
    'stats.today': '今天',
    'stats.last_24h': '最近24小时',
    'stats.yesterday': '昨天',
    'stats.last_7_days': '7天',
    'stats.last_30_days': '30天',
    'stats.total': '总计',
    'stats.total_requests': '总请求',
    'stats.input_tokens': '输入Token',
    'stats.output_tokens': '输出Token',
    'stats.rpm': 'RPM',
    'stats.success_rate': '成功率',
    'stats.avg_latency': '均延迟',
    'stats.token_trend': 'Token 用量趋势',
    'stats.request_trend': '请求量趋势',
    'stats.model_usage': '模型使用统计',
    'stats.account_usage': '账号使用统计',
    'stats.recent_requests': '最近请求',
    'stats.request_detail': '请求详情',
    'stats.no_data': '暂无数据',
    'stats.detail_empty': '请选择一条请求查看详情',
    'stats.filter_all': '全部',
    'stats.filter_success': '成功',
    'stats.filter_error': '失败',
    'stats.search_placeholder': '搜索路径、模型、账号...',
    'stats.switch_to_history': '查看历史',
    'stats.switch_to_recent': '查看最近',
    'stats.history_date': '日期',
    'stats.mode_recent': '最近请求',
    'stats.mode_history': '历史模式',
    'stats.mode_total': '{mode} · 共 {total} 条',
    'stats.th_model': '模型',
    'stats.th_requests': '请求数',
    'stats.th_input': '输入',
    'stats.th_output': '输出',
    'stats.th_cached': '缓存命中',
    'stats.th_reasoning': '推理',
    'stats.th_total': '总计',
    'stats.th_avg_latency': '均延迟',
    'stats.th_proportion': '占比',
    'stats.th_account': '账号',
    'stats.caller': '调用者',
    'stats.caller_identity.local': '本地访问',
    'stats.caller_identity.legacy_password': '密码认证',
    'stats.caller_identity.anonymous': '匿名',
    'stats.caller_identity.api_key': 'API密钥',
    'stats.th_errors': '错误',
    'stats.th_error_type': '错误类型',
    'stats.th_time': '时间',
    'stats.th_route': '路由',
    'stats.th_path': '路径',
    'stats.th_status': '状态',
    'stats.th_latency': '延迟',
    'stats.th_ttfb': '首字节',
    'stats.cache_total': '总缓存',
    'stats.cache_hit_rate': '缓存命中率',
    'stats.field_cached_tokens': '缓存Token',
    'stats.field_reasoning_tokens': '推理Token',
    'stats.field_cache_hit_rate': '缓存命中率',
    'stats.field_total_tokens': '总Token',
    'stats.field_stream': '流式',
    'stats.th_duration': '用时/首字',
    'stats.th_detail': '详情',
    'stats.showing': '显示第 {from} - {to} 条，共 {total} 条',
    'stats.per_page': '每页',
    'date.last1h': '近1小时',
    'date.last3h': '近3小时',
    'date.last6h': '近6小时',
    'date.last12h': '近12小时',
    'datepicker.from': '开始日期',
    'datepicker.to': '结束日期',
    'datepicker.apply': '应用',
    'dashboard.today_requests': '今日请求',
    'dashboard.today_tokens': '今日Token',
    'dashboard.current_rpm': '当前RPM',
    'dashboard.current_tpm': '当前TPM',
    'dashboard.more_info': '更多信息',
    'dashboard.success_rate': '成功率',

    // 仪表盘
    'dashboard.total': '总账号',
    'dashboard.active': '活跃',
    'dashboard.cooldown': '冷却中',
    'dashboard.banned': '已封禁',
    'dashboard.expired': '已过期',
    'dashboard.wasted': '已废弃',
    'dashboard.models_title': '可用模型',
    'dashboard.models_subtitle': '当前启用的 Codex 模型',
    'dashboard.service_title': '服务状态',
    'dashboard.service_subtitle': '运行时信息',
    'dashboard.uptime': '运行时间',
    'dashboard.scheduler': '调度模式',
    'dashboard.version': '版本',
    'dashboard.node_version': 'Node.js',
    'dashboard.no_models': '暂无模型',
    'dashboard.load_failed': '加载仪表盘失败',
    'dashboard.health_online': '服务在线',
    'dashboard.health_offline': '服务离线',
    'dashboard.health_checking': '检查中...',
    'dashboard.analytics_title': '数据分析',
    'dashboard.tab_token_trend': 'Token趋势',
    'dashboard.tab_request_trend': '请求趋势',
    'dashboard.tab_model_rank': '模型排行',
    'dashboard.tab_account_rank': '账号排行',
    'dashboard.tab_caller_rank': '调用者排行',
    'dashboard.test_models': '测试模型',
    'dashboard.testing_models': '测试中...',
    'dashboard.test_complete': '测试完成: {ok} 通过, {fail} 失败',
    'dashboard.test_ok': '通过',
    'dashboard.test_fail': '失败',
    'dashboard.test_error': '错误',
    'dashboard.test_latency': '{ms}ms',

    // 账号管理
    'accounts.import': '导入账号',
    'accounts.export': '导出账号',
    'accounts.refresh': '刷新',
    'accounts.search_placeholder': '搜索邮箱...',
    'accounts.filter_all': '全部',
    'accounts.filter_active': '活跃',
    'accounts.filter_cooldown': '冷却',
    'accounts.filter_banned': '封禁',
    'accounts.filter_expired': '过期',
    'accounts.filter_wasted': '废弃',
    'accounts.th_email': '邮箱',
    'accounts.th_status': '状态',
    'accounts.th_requests': '请求数',
    'accounts.th_token_expires': 'Token 过期',
    'accounts.th_errors': '连续错误',
    'accounts.th_last_error': '最近错误',
    'accounts.th_actions': '操作',
    'accounts.btn_refresh_token': '刷新',
    'accounts.btn_cooldown': '冷却',
    'accounts.btn_waste': '废弃',
    'accounts.btn_activate': '激活',
    'accounts.btn_verify': '验证',
    'accounts.verify_batch': '批量验证失效账号',
    'accounts.verifying': '正在验证…',
    'accounts.verify_result': '验证完成：{ok} 个恢复，{fail} 个废弃',
    'accounts.confirm_verify_batch': '确认批量验证 {count} 个失效账号？',
    'accounts.no_verifiable': '没有可验证的失效账号',

    'accounts.btn_check': '检测',
    'accounts.btn_test': '测试',
    'accounts.test_batch': '批量测试',
    'accounts.check_batch': '批量检测封禁',
    'accounts.checking': '正在检测…',
    'accounts.check_result_active': '正常',
    'accounts.check_result_banned': '已封禁',
    'accounts.check_result_error': '检测失败',
    'accounts.check_done': '检测完成：{active} 正常，{banned} 封禁，{error} 失败',
    'accounts.confirm_check_batch': '确认批量检测 {count} 个账号？',
    'accounts.no_checkable': '没有可检测的账号',
    'accounts.no_accounts': '暂无账号',
    'accounts.avg_lifespan': '平均寿命',
    'accounts.median_lifespan': '中位寿命',
    'accounts.alive_age': '存活年龄',
    'accounts.dead_count': '已失效',
    'accounts.alive_count': '存活中',
    'accounts.lifespan_banned': '封禁寿命',
    'accounts.lifespan_wasted': '废弃寿命',
    'accounts.lifespan_expired': '过期寿命',
    'accounts.no_match': '没有匹配的账号',
    'accounts.load_failed': '加载账号列表失败',
    'accounts.action_success': '操作成功',
    'accounts.action_failed': '操作失败',
    'accounts.confirm_waste': '确定要将此账号标记为废弃吗？',
    'accounts.import_title': '导入账号',
    'accounts.import_paste_label': '粘贴 JSON 数据',
    'accounts.import_placeholder': '[{"email":"...","accessToken":"...","sessionToken":"..."}]',
    'accounts.import_hint': '支持单个对象或数组格式，每个对象需包含 email、accessToken 或 sessionToken 字段',
    'accounts.import_file_hint': '或点击此处 / 拖拽上传 JSON 文件',
    'accounts.import_cancel': '取消',
    'accounts.import_confirm': '确认导入',
    'accounts.import_success': '导入成功',
    'accounts.import_failed': '导入失败',
    'accounts.import_json_error': 'JSON 格式错误',
    'accounts.import_empty': '请输入 JSON 数据',
    'accounts.export_success': '导出成功',
    'accounts.export_failed': '导出失败',
    'accounts.refreshed': '已刷新',
    'accounts.btn_delete': '删除',
    'accounts.confirm_delete': '确定要彻底删除此账号吗？此操作不可撤销。',
    'accounts.delete_success': '删除成功',
    'accounts.delete_failed': '删除失败',
    'accounts.deleted': '账号已删除: {email}',
    'accounts.gpa_title': '凭证导入/导出',
    'accounts.gpa_desc': '导入 GPA Codex 凭证，支持预览与导出',
    'accounts.gpa_file_label': '上传 JSON 文件（可多选）',
    'accounts.gpa_file_hint': '未选择文件时使用下方文本',
    'accounts.gpa_file_selected': '已选择 {count} 个文件',
    'accounts.gpa_file_read_failed': '读取文件失败',
    'accounts.gpa_text_label': '粘贴 JSON',
    'accounts.gpa_text_placeholder': '[{\"type\":\"codex\",\"email\":\"user@example.com\",\"access_token\":\"...\"}]',
    'accounts.gpa_result_title': '处理结果',
    'accounts.gpa_preview': '预览',
    'accounts.gpa_preview_success': '预览完成',
    'accounts.gpa_preview_failed': '预览失败',
    'accounts.gpa_import': '导入',
    'accounts.gpa_import_empty': '请上传 JSON 文件或粘贴 JSON 内容',
    'accounts.gpa_import_done': '导入完成：新增 {imported}，更新 {updated}，拒绝 {rejected}',
    'accounts.gpa_import_failed': '导入失败',
    'accounts.gpa_export': '导出 GPA 格式',
    'accounts.gpa_export_success': '导出完成：{count} 个凭证',
    'accounts.gpa_export_failed': '导出失败',

    // 状态标签
    'status.active': '活跃',
    'status.cooldown': '冷却中',
    'status.banned': '已封禁',
    'status.expired': '已过期',
    'status.relogin_needed': '已停用',
    'status.wasted': '已废弃',

    // 错误类型
    'error.token_expired': 'Token过期',
    'error.ip_blocked': 'IP封锁',
    'error.mfa_needed': '需要MFA',
    'error.account_banned': '账号封禁',
    'error.rate_limited': '频率限制',
    'error.usage_limited': '额度已用尽（按提示时间后重试）',
    'error.session_invalidated': '会话失效（已登出或切换账号）',
    'error.bad_request': '请求参数错误',
    'error.upstream_error': '上游错误',
    'error.upstream_unavailable': '上游不可用',
    'error.upstream_overloaded': '上游过载',
    'error.network_error': '网络错误',

    // 配置
    'config.title': '配置管理',
    'config.save': '保存配置',
    'config.reload': '重新加载',
    'config.save_success': '配置已保存',
    'config.save_failed': '保存失败',
    'config.sensitive_confirm': '修改敏感配置（密码/用户名）需要验证管理员密码',
    'config.sensitive_title': '安全验证',
    'config.load_failed': '加载配置失败',
    'config.saving': '正在保存配置...',
    'config.section_server': '服务器设置',
    'config.section_upstream': '上游设置',
    'config.section_models': '模型设置',
    'config.section_scheduler': '调度设置',
    'config.section_retry': '重试设置',
    'config.section_rate_limit': '限流设置',
    'config.section_health_check': '健康检查',
    'config.section_credentials': '凭证设置',
    'config.section_proxy': '代理设置',
    'config.proxy_local_title': '本机代理',
    'config.proxy_enabled': '启用代理',
    'config.proxy_preset': '代理预设',
    'config.proxy_custom': '自定义',
    'config.proxy_node_select': '单节点选择',
    'config.proxy_current': '当前代理',
    'config.proxy_test': '测试连通性',
    'config.proxy_test_success': '出口IP: {ip}',
    'config.proxy_test_fail': '连接失败',
    'config.proxy_host': '代理主机',
    'config.proxy_username': '认证用户',
    'config.proxy_password': '认证密码',
    'config.proxy_updated': '代理已切换',
    'config.register_proxy_title': '注册机代理',
    'config.register_proxy_sync': '跟随本地代理',
    'config.register_proxy_enabled': '启用注册机代理',
    'config.register_proxy_server': '注册机代理地址',
    'config.register_proxy_updated': '注册机代理已更新',
    'config.totp_section': 'TOTP 双因素认证',
    'config.totp_refresh_status': '刷新状态',
    'config.totp_status_label': '当前状态',
    'config.totp_enable_title': '启用 TOTP',
    'config.totp_enable_desc': '先输入管理员密码初始化密钥，再输入认证器验证码确认启用。',
    'config.totp_setup_init': '初始化密钥',
    'config.totp_setup_init_loading': '正在初始化 TOTP...',
    'config.totp_setup_init_success': 'TOTP 初始化成功',
    'config.totp_setup_init_failed': 'TOTP 初始化失败',
    'config.totp_secret': 'TOTP Secret',
    'config.totp_uri': 'otpauth URI',
    'config.totp_code': '验证码',
    'config.totp_code_placeholder': '请输入 6 位验证码',
    'config.totp_code_required': '请输入验证码',
    'config.totp_setup_confirm': '确认并启用',
    'config.totp_setup_confirm_loading': '正在确认启用...',
    'config.totp_setup_confirm_success': 'TOTP 已启用',
    'config.totp_setup_confirm_failed': 'TOTP 启用失败',
    'config.totp_disable_title': '禁用 TOTP',
    'config.totp_disable_desc': '输入管理员密码和当前验证码后可关闭双因素登录。',
    'config.totp_disable': '禁用 TOTP',
    'config.totp_disable_loading': '正在禁用 TOTP...',
    'config.totp_disable_success': 'TOTP 已禁用',
    'config.totp_disable_failed': 'TOTP 禁用失败',
    'config.totp_configured': '已配置密钥',
    'config.totp_not_configured': '未配置密钥',
    'config.totp_status_load_failed': '获取 TOTP 状态失败',
    'config.host': '监听地址',
    'config.host_tip': '服务器监听的 IP 地址，0.0.0.0 表示所有接口',
    'config.port': '端口',
    'config.port_tip': '服务器监听的端口号',
    'config.api_password': 'API 访问密码',
    'config.api_password_tip': '外部 API 路由的访问密码（Bearer Token）',
    'config.admin_username': '管理员用户名',
    'config.admin_username_tip': '管理面板登录用户名',
    'config.admin_password': '管理员密码',
    'config.admin_password_tip': '管理面板登录密码，建议至少 6 位',
    'config.base_url': '上游地址',
    'config.base_url_tip': 'ChatGPT API 基础地址',
    'config.timeout': '请求超时 (ms)',
    'config.timeout_tip': '上游请求超时时间，单位毫秒',
    'config.stream_timeout': '流超时 (ms)',
    'config.stream_timeout_tip': '流式请求超时时间，单位毫秒',
    'config.model_prefix': '模型前缀',
    'config.model_prefix_tip': '模型名称前缀，用于映射',
    'config.default_model': '默认模型',
    'config.default_model_tip': '未指定模型时使用的默认模型',
    'config.models_section': '模型管理',
    'config.models_desc': '可在管理面板直接增删改模型并热更新，无需重启服务',
    'config.models_prefix': '模型前缀',
    'config.models_default': '默认模型',
    'config.models_available_title': '可用模型列表',
    'config.models_aliases_title': '模型别名',
    'config.models_add': '新增模型',
    'config.models_alias_add': '新增别名',
    'config.models_reload': '刷新模型配置',
    'config.models_discovery_refresh': '刷新上游模型',
    'config.models_discovery_refreshing': '正在拉取上游模型...',
    'config.models_discovery_refresh_success': '上游模型刷新成功',
    'config.models_discovery_refresh_failed': '上游模型刷新失败',
    'config.models_save': '保存模型配置',
    'config.models_saving': '正在保存模型配置...',
    'config.models_save_success': '模型配置已保存',
    'config.models_save_failed': '模型配置保存失败',
    'config.models_load_failed': '加载模型配置失败',
    'config.models_empty': '暂无模型，请先新增至少一个模型',
    'config.models_alias_empty': '暂无别名',
    'config.models_col_name': '模型名',
    'config.models_col_display': '显示名',
    'config.models_col_enabled': '启用',
    'config.models_col_actions': '操作',
    'config.models_col_alias': '别名',
    'config.models_col_target': '目标模型',
    'config.models_missing_name': '模型别名不能为空',
    'config.models_missing_target': '模型别名目标不能为空',
    'config.scheduler_mode': '调度模式',
    'config.scheduler_mode_tip': '账号轮询调度策略',
    'config.scheduler_round_robin': '轮询',
    'config.scheduler_random': '随机',
    'config.scheduler_least_used': '最少使用',
    'config.max_retries': '最大重试次数',
    'config.max_retries_tip': '请求失败时的最大重试次数',
    'config.backoff_ms': '退避时间 (ms)',
    'config.backoff_ms_tip': '重试之间的退避等待时间',
    'config.rpm': '每分钟请求数',
    'config.rpm_tip': '每分钟允许的最大请求数',
    'config.cooldown_ms': '冷却时间 (ms)',
    'config.cooldown_ms_tip': '账号冷却持续时间',
    'config.health_enabled': '启用健康检查',
    'config.health_enabled_tip': '是否定期检查账号状态',
    'config.health_interval': '检查间隔 (分钟)',
    'config.health_interval_tip': '健康检查的执行间隔',
    'config.auto_refresh': '自动刷新凭证',
    'config.auto_refresh_tip': '是否在过期前自动刷新 Token',
    'config.refresh_before': '提前刷新 (秒)',
    'config.refresh_before_tip': '在 Token 过期前多少秒开始刷新',
    'config.api_token': '凭证 API Token',
    'config.api_token_tip': '用于 POST /api/credentials 接口认证的长字符串',
    'config.api_keys_title': 'API Keys 管理',
    'config.api_keys_desc': '为不同调用方创建独立 API Key',
    'config.api_keys_add': '新建 API Key',
    'config.api_keys_refresh': '刷新 API Keys',
    'config.api_keys_empty': '暂无 API Key',
    'config.api_key_id': 'Key ID',
    'config.api_key_identity': '身份标识',
    'config.api_key_key': 'Key',
    'config.api_key_enabled': '启用',
    'config.api_key_created_at': '创建时间',
    'config.api_key_actions': '操作',
    'config.api_key_enable': '启用',
    'config.api_key_disable': '禁用',
    'config.api_key_rotate': '轮换',
    'config.api_key_delete': '删除',
    'config.api_key_created': 'API Key 已创建',
    'config.api_key_rotated': 'API Key 已轮换',
    'config.api_key_deleted': 'API Key 已删除',
    'config.api_key_create_failed': '创建 API Key 失败',
    'config.api_key_update_failed': '更新 API Key 失败',
    'config.api_key_delete_failed': '删除 API Key 失败',
    'config.api_key_rotate_failed': '轮换 API Key 失败',
    'config.api_key_plaintext_tip': '请立即保存明文 Key，此后无法再次查看',

    // 日志
    'logs.title': '请求日志',
    'logs.search_placeholder': '搜索日志...',
    'logs.filter_all': '全部',
    'logs.filter_info': '信息',
    'logs.filter_warn': '警告',
    'logs.filter_error': '错误',
    'logs.filter_request': '请求',
    'logs.auto_refresh': '自动刷新',
    'logs.auto_refresh_stop': '停止刷新',
    'logs.manual_refresh': '刷新',
    'logs.clear': '清空',
    'logs.clear_confirm': '确定要清空所有日志吗？此操作不可恢复。',
    'logs.clear_title': '清空日志',
    'logs.clear_success': '日志已清空',
    'logs.clear_failed': '清空日志失败',
    'logs.load_more': '加载更多',
    'logs.no_logs': '暂无日志',
    'logs.load_failed': '加载日志失败',
    'logs.copied': '已复制到剪贴板',
    'logs.copy_failed': '复制失败',
    'logs.copy': '复制',

    // 风控
    'abuse.users_title': '风险用户',
    'abuse.events_title': '风险事件流',
    'abuse.rules_title': '规则配置',
    'abuse.save_rules': '保存规则',
    'abuse.filter_all': '全部等级',
    'abuse.level_low': '低',
    'abuse.level_medium': '中',
    'abuse.level_high': '高',
    'abuse.level_critical': '严重',
    'abuse.sort_score_desc': '分数降序',
    'abuse.sort_score_asc': '分数升序',
    'abuse.sort_updated_desc': '最近评估',
    'abuse.search_placeholder': '搜索身份/用户名',
    'abuse.refresh': '刷新',
    'abuse.col_seq_id': '序号',
    'abuse.col_username': '用户名',
    'abuse.col_requests': '请求数',
    'abuse.col_input_tokens': '输入Token',
    'abuse.col_output_tokens': '输出Token',
    'abuse.col_cached_tokens': '缓存Token',
    'abuse.col_last_active': '最后活跃',
    'abuse.history_title': '请求历史',
    'abuse.col_time': '时间',
    'abuse.col_model': '模型',
    'abuse.col_status': '状态码',
    'abuse.col_latency': '延迟(ms)',
    'abuse.col_ip': 'IP',
    'abuse.load_more': '加载更多',
    'abuse.no_history': '暂无请求记录',
    'abuse.th_identity': '身份',
    'abuse.th_score': '风险分',
    'abuse.th_level': '等级',
    'abuse.th_action': '动作',
    'abuse.th_reasons': '命中规则',
    'abuse.th_time': '最近评估',
    'abuse.th_actions': '操作',
    'abuse.th_rule': '规则',
    'abuse.th_value': '当前值',
    'abuse.th_threshold': '阈值',
    'abuse.view_detail': '详情',
    'abuse.card_total_users': '用户总数',
    'abuse.card_risk_users': '风险身份数',
    'abuse.card_critical_users': '严重风险',
    'abuse.card_suspend_users': '封禁中',
    'abuse.card_today_events': '今日事件',
    'abuse.users_empty': '暂无风险用户',
    'abuse.events_empty': '暂无风险事件',
    'abuse.detail_title': '风险详情',
    'abuse.detail_rules': '命中规则',
    'abuse.detail_timeline': '事件时间线',
    'abuse.rules_empty': '暂无命中规则',
    'abuse.load_failed': '加载风控数据失败',
    'abuse.invalid_rules_json': '规则 JSON 格式错误',
    'abuse.rules_saved': '规则已保存',
    'abuse.save_failed': '保存失败',
    'abuse.confirm_action': '确认执行动作：{action}？',
    'abuse.action_done': '动作执行成功',
    'abuse.action_failed': '动作执行失败',
    'abuse.action_observe': '观察',
    'abuse.action_throttle': '限速',
    'abuse.action_challenge': '挑战',
    'abuse.action_suspend': '封禁',
    'abuse.action_restore': '恢复',

    // 账号登录
    'accounts.browser_login': '账号登录',
    'accounts.browser_login_title': '账号登录添加',
    'accounts.browser_login_desc': '输入 ChatGPT 邮箱和密码，服务端自动登录并提取凭证',
    'accounts.browser_login_email': '邮箱',
    'accounts.browser_login_email_placeholder': '输入 ChatGPT 邮箱',
    'accounts.browser_login_password': '密码',
    'accounts.browser_login_password_placeholder': '输入密码',
    'accounts.browser_login_submit': '登录并提取凭证',
    'accounts.browser_login_submitting': '正在登录...',
    'accounts.browser_login_success': '登录成功，凭证已添加',
    'accounts.browser_login_failed': '登录失败',
    'accounts.browser_login_empty': '请输入邮箱和密码',

    // 主题
    'theme.toggle': '切换主题',
    'theme.light': '浅色模式',
    'theme.dark': '深色模式',

    // 通用
    'common.confirm': '确定',
    'common.cancel': '取消',
    'common.loading': '加载中...',
    'common.save': '保存',
    'common.delete': '删除',
    'common.edit': '编辑',
    'common.close': '关闭',
    'common.yes': '是',
    'common.no': '否',
    'common.enabled': '启用',
    'common.disabled': '禁用',
    'common.logout': '退出登录',
    'common.version_label': 'codex2api',
    'common.success': '成功',
    'common.error': '错误',
    'common.warning': '警告',
    'common.email_show': '显示邮箱',
    'common.email_hide': '隐藏邮箱',
    'common.admin_password': '管理员密码',
    'common.enter_admin_password': '请输入管理员密码',
    'common.password_required': '需要输入管理员密码',

    // 时间
    'time.days': '天',
    'time.hours': '小时',
    'time.minutes': '分钟',
    'time.just_now': '刚刚',
    'time.seconds_ago': '{n}秒前',
    'time.minutes_ago': '{n}分钟前',
    'time.hours_ago': '{n}小时前',
    'time.days_ago': '{n}天前',
    'time.ms': 'ms',
  },
  en: {
    // 登录
    'login.title': 'codex2api',
    'login.subtitle': 'Admin Panel',
    'login.username': 'Username',
    'login.password': 'Password',
    'login.username_placeholder': 'Enter username',
    'login.password_placeholder': 'Enter password',
    'login.submit': 'Login',
    'login.submitting': 'Logging in...',
    'login.error_empty': 'Please enter username and password',
    'login.success': 'Login successful',
    'login.failed': 'Login failed',
    'login.expired': 'Session expired, please login again',
    'login.logged_out': 'Logged out',
    'login.mode': 'Login Mode',
    'login.mode_password_totp': 'Password + TOTP',
    'login.mode_totp_only': 'TOTP Only',
    'login.totp_code': 'TOTP Code',
    'login.totp_placeholder': 'Enter {digits}-digit code',
    'login.totp_required': 'Please enter TOTP code',

    // 导航
    'nav.dashboard': 'Dashboard',
    'nav.accounts': 'Accounts',
    'nav.statistics': 'Statistics',
    'nav.config': 'Config',
    'nav.logs': 'Logs',
    'nav.registration': 'Register',
    'nav.abuse': 'Abuse',

    // Registration
    'registration.checking': 'Checking connection...',
    'registration.server_online': 'Register server online',
    'registration.server_offline': 'Register server offline',
    'registration.count': 'Count',
    'registration.concurrency': 'Concurrency',
    'registration.proxy': 'Proxy',
    'registration.auto_upload': 'Auto Upload',
    'registration.start': 'Start',
    'registration.stop': 'Stop',
    'registration.progress': 'Progress',
    'registration.success': 'Success',
    'registration.failed': 'Failed',
    'registration.uncertain': 'Uncertain',
    'registration.total': 'Total',
    'registration.with_token': 'Has Token',
    'registration.remote_accounts': 'Remote Accounts',
    'registration.registered_at': 'Registered At',
    'registration.no_accounts': 'No registration records',
    'registration.load_failed': 'Failed to load',
    'registration.confirm_start': 'Start registering {count} accounts?',
    'registration.job_started': 'Registration job started: {count}',
    'registration.start_failed': 'Failed to start',
    'registration.job_stopping': 'Stopping job...',
    'registration.stop_failed': 'Failed to stop',
    'registration.job_completed': 'Registration completed',
    'registration.invalid_count': 'Count must be between 1-200',
    'registration.monitor_title': 'Registration Success Rate',
    'registration.step_mail_create': 'Mail Create',
    'registration.step_oauth_init': 'OAuth Init',
    'registration.step_sentinel_reg': 'Sentinel #1',
    'registration.step_email_otp': 'Email OTP',
    'registration.step_sentinel_create': 'Sentinel #2',
    'registration.step_oauth_callback': 'OAuth Callback',
    'registration.overall_rate': 'Overall Rate',
    'registration.no_stats': 'No stats available',
    'registration.history_title': 'Job History',
    'registration.history_empty': 'No history records',
    'registration.history_job_id': 'Job ID',
    'registration.history_time': 'Time',
    'registration.history_count': 'Count',
    'registration.history_result': 'Result',
    'registration.history_elapsed': 'Elapsed',
    'registration.history_detail': 'Step Stats',
    'pool_health.title': 'Pool Health Monitor',
    'pool_health.probe_status': 'Probe Status',
    'pool_health.guard_status': 'Guard Status',
    'pool_health.registering': 'Registering',
    'pool_health.idle': 'Idle',

    // 统计
    'stats.today': 'Today',
    'stats.last_24h': 'Last 24 Hours',
    'stats.yesterday': 'Yesterday',
    'stats.last_7_days': '7 Days',
    'stats.last_30_days': '30 Days',
    'stats.total': 'Total',
    'stats.total_requests': 'Total Requests',
    'stats.input_tokens': 'Input Tokens',
    'stats.output_tokens': 'Output Tokens',
    'stats.rpm': 'RPM',
    'stats.success_rate': 'Success Rate',
    'stats.avg_latency': 'Avg Latency',
    'stats.token_trend': 'Token Usage Trend',
    'stats.request_trend': 'Request Trend',
    'stats.model_usage': 'Model Usage',
    'stats.account_usage': 'Account Usage',
    'stats.recent_requests': 'Recent Requests',
    'stats.request_detail': 'Request Detail',
    'stats.no_data': 'No data available',
    'stats.detail_empty': 'Select a request to view details',
    'stats.filter_all': 'All',
    'stats.filter_success': 'Success',
    'stats.filter_error': 'Failed',
    'stats.search_placeholder': 'Search path, model, account...',
    'stats.switch_to_history': 'View History',
    'stats.switch_to_recent': 'View Recent',
    'stats.history_date': 'Date',
    'stats.mode_recent': 'Recent Requests',
    'stats.mode_history': 'History Mode',
    'stats.mode_total': '{mode} · {total} items',
    'stats.th_model': 'Model',
    'stats.th_requests': 'Requests',
    'stats.th_input': 'Input',
    'stats.th_output': 'Output',
    'stats.th_cached': 'Cache Hit',
    'stats.th_reasoning': 'Reasoning',
    'stats.th_total': 'Total',
    'stats.th_avg_latency': 'Avg Latency',
    'stats.th_proportion': 'Proportion',
    'stats.th_account': 'Account',
    'stats.caller': 'Caller',
    'stats.caller_identity.local': 'Local',
    'stats.caller_identity.legacy_password': 'Password',
    'stats.caller_identity.anonymous': 'Anonymous',
    'stats.caller_identity.api_key': 'API Key',
    'stats.th_errors': 'Errors',
    'stats.th_error_type': 'Error Type',
    'stats.th_time': 'Time',
    'stats.th_route': 'Route',
    'stats.th_path': 'Path',
    'stats.th_status': 'Status',
    'stats.th_latency': 'Latency',
    'stats.th_ttfb': 'TTFB',
    'stats.cache_total': 'Total Cached',
    'stats.cache_hit_rate': 'Cache Hit Rate',
    'stats.field_cached_tokens': 'Cached Tokens',
    'stats.field_reasoning_tokens': 'Reasoning Tokens',
    'stats.field_cache_hit_rate': 'Cache Hit Rate',
    'stats.field_total_tokens': 'Total Tokens',
    'stats.field_stream': 'Stream',
    'stats.th_duration': 'Duration/TTFB',
    'stats.th_detail': 'Details',
    'stats.showing': 'Showing {from} - {to} of {total}',
    'stats.per_page': 'Per page',
    'date.last1h': 'Last 1h',
    'date.last3h': 'Last 3h',
    'date.last6h': 'Last 6h',
    'date.last12h': 'Last 12h',
    'datepicker.from': 'From',
    'datepicker.to': 'To',
    'datepicker.apply': 'Apply',
    'dashboard.today_requests': 'Today Requests',
    'dashboard.today_tokens': 'Today Tokens',
    'dashboard.current_rpm': 'Current RPM',
    'dashboard.current_tpm': 'Current TPM',
    'dashboard.more_info': 'More Info',
    'dashboard.success_rate': 'Success Rate',

    // 仪表盘
    'dashboard.total': 'Total',
    'dashboard.active': 'Active',
    'dashboard.cooldown': 'Cooldown',
    'dashboard.banned': 'Banned',
    'dashboard.expired': 'Expired',
    'dashboard.wasted': 'Wasted',
    'dashboard.models_title': 'Available Models',
    'dashboard.models_subtitle': 'Currently enabled Codex models',
    'dashboard.service_title': 'Service Status',
    'dashboard.service_subtitle': 'Runtime information',
    'dashboard.uptime': 'Uptime',
    'dashboard.scheduler': 'Scheduler',
    'dashboard.version': 'Version',
    'dashboard.node_version': 'Node.js',
    'dashboard.no_models': 'No models available',
    'dashboard.load_failed': 'Failed to load dashboard',
    'dashboard.health_online': 'Service online',
    'dashboard.health_offline': 'Service offline',
    'dashboard.health_checking': 'Checking...',
    'dashboard.analytics_title': 'Data Analytics',
    'dashboard.tab_token_trend': 'Token Trend',
    'dashboard.tab_request_trend': 'Request Trend',
    'dashboard.tab_model_rank': 'Model Ranking',
    'dashboard.tab_account_rank': 'Account Ranking',
    'dashboard.tab_caller_rank': 'Caller Ranking',
    'dashboard.test_models': 'Test Models',
    'dashboard.testing_models': 'Testing...',
    'dashboard.test_complete': 'Test complete: {ok} passed, {fail} failed',
    'dashboard.test_ok': 'Pass',
    'dashboard.test_fail': 'Fail',
    'dashboard.test_error': 'Error',
    'dashboard.test_latency': '{ms}ms',

    // 账号管理
    'accounts.import': 'Import',
    'accounts.export': 'Export',
    'accounts.refresh': 'Refresh',
    'accounts.search_placeholder': 'Search email...',
    'accounts.filter_all': 'All',
    'accounts.filter_active': 'Active',
    'accounts.filter_cooldown': 'Cooldown',
    'accounts.filter_banned': 'Banned',
    'accounts.filter_expired': 'Expired',
    'accounts.filter_wasted': 'Wasted',
    'accounts.th_email': 'Email',
    'accounts.th_status': 'Status',
    'accounts.th_requests': 'Requests',
    'accounts.th_token_expires': 'Token Expires',
    'accounts.th_errors': 'Errors',
    'accounts.th_last_error': 'Last Error',
    'accounts.th_actions': 'Actions',
    'accounts.btn_refresh_token': 'Refresh',
    'accounts.btn_cooldown': 'Cooldown',
    'accounts.btn_waste': 'Waste',
    'accounts.btn_activate': 'Activate',
    'accounts.btn_verify': 'Verify',
    'accounts.verify_batch': 'Batch Verify Failed Accounts',
    'accounts.verifying': 'Verifying...',
    'accounts.verify_result': 'Verification done: {ok} restored, {fail} wasted',
    'accounts.confirm_verify_batch': 'Verify {count} failed accounts?',
    'accounts.no_verifiable': 'No verifiable accounts found',

    'accounts.btn_check': 'Check',
    'accounts.btn_test': 'Test',
    'accounts.test_batch': 'Batch Test',
    'accounts.check_batch': 'Batch Check Ban',
    'accounts.checking': 'Checking...',
    'accounts.check_result_active': 'Active',
    'accounts.check_result_banned': 'Banned',
    'accounts.check_result_error': 'Check Failed',
    'accounts.check_done': 'Check done: {active} active, {banned} banned, {error} failed',
    'accounts.confirm_check_batch': 'Check ban status of {count} accounts?',
    'accounts.no_checkable': 'No checkable accounts',
    'accounts.no_accounts': 'No accounts',
    'accounts.avg_lifespan': 'Avg Lifespan',
    'accounts.median_lifespan': 'Median Lifespan',
    'accounts.alive_age': 'Alive Age',
    'accounts.dead_count': 'Dead',
    'accounts.alive_count': 'Alive',
    'accounts.lifespan_banned': 'Banned Lifespan',
    'accounts.lifespan_wasted': 'Wasted Lifespan',
    'accounts.lifespan_expired': 'Expired Lifespan',
    'accounts.no_match': 'No matching accounts',
    'accounts.load_failed': 'Failed to load accounts',
    'accounts.action_success': 'Action completed',
    'accounts.action_failed': 'Action failed',
    'accounts.confirm_waste': 'Are you sure you want to mark this account as wasted?',
    'accounts.import_title': 'Import Accounts',
    'accounts.import_paste_label': 'Paste JSON data',
    'accounts.import_placeholder': '[{"email":"...","accessToken":"...","sessionToken":"..."}]',
    'accounts.import_hint': 'Supports single object or array format, each object must include email, accessToken or sessionToken',
    'accounts.import_file_hint': 'Or click here / drag and drop JSON file',
    'accounts.import_cancel': 'Cancel',
    'accounts.import_confirm': 'Confirm Import',
    'accounts.import_success': 'Import successful',
    'accounts.import_failed': 'Import failed',
    'accounts.import_json_error': 'Invalid JSON format',
    'accounts.import_empty': 'Please enter JSON data',
    'accounts.export_success': 'Export successful',
    'accounts.export_failed': 'Export failed',
    'accounts.refreshed': 'Refreshed',
    'accounts.btn_delete': 'Delete',
    'accounts.confirm_delete': 'Are you sure you want to permanently delete this account? This cannot be undone.',
    'accounts.delete_success': 'Deleted',
    'accounts.delete_failed': 'Delete failed',
    'accounts.deleted': 'Account deleted: {email}',
    'accounts.gpa_title': 'Credential Import/Export',
    'accounts.gpa_desc': 'Import GPA Codex credentials with preview and export support',
    'accounts.gpa_file_label': 'Upload JSON Files (multi-select)',
    'accounts.gpa_file_hint': 'If no file is selected, pasted JSON below will be used',
    'accounts.gpa_file_selected': '{count} file(s) selected',
    'accounts.gpa_file_read_failed': 'Failed to read file',
    'accounts.gpa_text_label': 'Paste JSON',
    'accounts.gpa_text_placeholder': '[{\"type\":\"codex\",\"email\":\"user@example.com\",\"access_token\":\"...\"}]',
    'accounts.gpa_result_title': 'Result',
    'accounts.gpa_preview': 'Preview',
    'accounts.gpa_preview_success': 'Preview completed',
    'accounts.gpa_preview_failed': 'Preview failed',
    'accounts.gpa_import': 'Import',
    'accounts.gpa_import_empty': 'Please upload JSON files or paste JSON content',
    'accounts.gpa_import_done': 'Import done: {imported} imported, {updated} updated, {rejected} rejected',
    'accounts.gpa_import_failed': 'Import failed',
    'accounts.gpa_export': 'Export GPA',
    'accounts.gpa_export_success': 'Exported {count} credentials',
    'accounts.gpa_export_failed': 'Export failed',

    // 状态标签
    'status.active': 'Active',
    'status.cooldown': 'Cooldown',
    'status.banned': 'Banned',
    'status.expired': 'Expired',
    'status.relogin_needed': 'Disabled',
    'status.wasted': 'Wasted',

    // 错误类型
    'error.token_expired': 'Token Expired',
    'error.ip_blocked': 'IP Blocked',
    'error.mfa_needed': 'MFA Required',
    'error.account_banned': 'Account Banned',
    'error.rate_limited': 'Rate Limited',
    'error.usage_limited': 'Usage limit reached (retry later)',
    'error.session_invalidated': 'Session Invalidated (logged out or switched account)',
    'error.bad_request': 'Bad request',
    'error.upstream_error': 'Upstream Error',
    'error.upstream_unavailable': 'Upstream Unavailable',
    'error.upstream_overloaded': 'Upstream Overloaded',
    'error.network_error': 'Network Error',

    // 配置
    'config.title': 'Configuration',
    'config.save': 'Save Config',
    'config.reload': 'Reload',
    'config.save_success': 'Config saved',
    'config.save_failed': 'Save failed',
    'config.sensitive_confirm': 'Changing sensitive settings (password/username) requires admin password verification',
    'config.sensitive_title': 'Security Verification',
    'config.load_failed': 'Failed to load config',
    'config.saving': 'Saving config...',
    'config.section_server': 'Server Settings',
    'config.section_upstream': 'Upstream Settings',
    'config.section_models': 'Model Settings',
    'config.section_scheduler': 'Scheduler Settings',
    'config.section_retry': 'Retry Settings',
    'config.section_rate_limit': 'Rate Limit Settings',
    'config.section_health_check': 'Health Check',
    'config.section_credentials': 'Credentials Settings',
    'config.section_proxy': 'Proxy Settings',
    'config.proxy_local_title': 'Local Proxy',
    'config.proxy_enabled': 'Enable Proxy',
    'config.proxy_preset': 'Proxy Preset',
    'config.proxy_custom': 'Custom',
    'config.proxy_node_select': 'Node Selection',
    'config.proxy_current': 'Current Proxy',
    'config.proxy_test': 'Test Connection',
    'config.proxy_test_success': 'Exit IP: {ip}',
    'config.proxy_test_fail': 'Failed',
    'config.proxy_host': 'Proxy Host',
    'config.proxy_username': 'Username',
    'config.proxy_password': 'Password',
    'config.proxy_updated': 'Proxy switched',
    'config.register_proxy_title': 'Registration Proxy',
    'config.register_proxy_sync': 'Sync with local',
    'config.register_proxy_enabled': 'Enable registration proxy',
    'config.register_proxy_server': 'Registration proxy address',
    'config.register_proxy_updated': 'Registration proxy updated',
    'config.totp_section': 'TOTP Two-Factor Authentication',
    'config.totp_refresh_status': 'Refresh Status',
    'config.totp_status_label': 'Current Status',
    'config.totp_enable_title': 'Enable TOTP',
    'config.totp_enable_desc': 'Enter admin password to initialize secret, then enter authenticator code to confirm.',
    'config.totp_setup_init': 'Initialize Secret',
    'config.totp_setup_init_loading': 'Initializing TOTP...',
    'config.totp_setup_init_success': 'TOTP initialized',
    'config.totp_setup_init_failed': 'TOTP init failed',
    'config.totp_secret': 'TOTP Secret',
    'config.totp_uri': 'otpauth URI',
    'config.totp_code': 'Verification Code',
    'config.totp_code_placeholder': 'Enter 6-digit code',
    'config.totp_code_required': 'Please enter verification code',
    'config.totp_setup_confirm': 'Confirm and Enable',
    'config.totp_setup_confirm_loading': 'Confirming TOTP setup...',
    'config.totp_setup_confirm_success': 'TOTP enabled',
    'config.totp_setup_confirm_failed': 'TOTP enable failed',
    'config.totp_disable_title': 'Disable TOTP',
    'config.totp_disable_desc': 'Enter admin password and current code to disable two-factor login.',
    'config.totp_disable': 'Disable TOTP',
    'config.totp_disable_loading': 'Disabling TOTP...',
    'config.totp_disable_success': 'TOTP disabled',
    'config.totp_disable_failed': 'TOTP disable failed',
    'config.totp_configured': 'Secret configured',
    'config.totp_not_configured': 'Secret not configured',
    'config.totp_status_load_failed': 'Failed to load TOTP status',
    'config.host': 'Listen Address',
    'config.host_tip': 'Server listen IP address, 0.0.0.0 for all interfaces',
    'config.port': 'Port',
    'config.port_tip': 'Server listen port',
    'config.api_password': 'API Password',
    'config.api_password_tip': 'API route access password (Bearer Token)',
    'config.admin_username': 'Admin Username',
    'config.admin_username_tip': 'Admin panel login username',
    'config.admin_password': 'Admin Password',
    'config.admin_password_tip': 'Admin panel login password, at least 6 characters recommended',
    'config.base_url': 'Upstream URL',
    'config.base_url_tip': 'ChatGPT API base URL',
    'config.timeout': 'Request Timeout (ms)',
    'config.timeout_tip': 'Upstream request timeout in milliseconds',
    'config.stream_timeout': 'Stream Timeout (ms)',
    'config.stream_timeout_tip': 'Stream request timeout in milliseconds',
    'config.model_prefix': 'Model Prefix',
    'config.model_prefix_tip': 'Model name prefix for mapping',
    'config.default_model': 'Default Model',
    'config.default_model_tip': 'Default model when not specified',
    'config.models_section': 'Model Management',
    'config.models_desc': 'Add, remove and edit models directly from admin panel with hot reload',
    'config.models_prefix': 'Model Prefix',
    'config.models_default': 'Default Model',
    'config.models_available_title': 'Available Models',
    'config.models_aliases_title': 'Model Aliases',
    'config.models_add': 'Add Model',
    'config.models_alias_add': 'Add Alias',
    'config.models_reload': 'Reload Models',
    'config.models_discovery_refresh': 'Refresh Upstream Models',
    'config.models_discovery_refreshing': 'Refreshing upstream models...',
    'config.models_discovery_refresh_success': 'Upstream models refreshed',
    'config.models_discovery_refresh_failed': 'Failed to refresh upstream models',
    'config.models_save': 'Save Models',
    'config.models_saving': 'Saving model configuration...',
    'config.models_save_success': 'Model configuration saved',
    'config.models_save_failed': 'Failed to save model configuration',
    'config.models_load_failed': 'Failed to load model configuration',
    'config.models_empty': 'No models configured, add at least one model',
    'config.models_alias_empty': 'No aliases',
    'config.models_col_name': 'Model ID',
    'config.models_col_display': 'Display Name',
    'config.models_col_enabled': 'Enabled',
    'config.models_col_actions': 'Actions',
    'config.models_col_alias': 'Alias',
    'config.models_col_target': 'Target Model',
    'config.models_missing_name': 'Alias name is required',
    'config.models_missing_target': 'Alias target is required',
    'config.scheduler_mode': 'Scheduler Mode',
    'config.scheduler_mode_tip': 'Account rotation scheduling strategy',
    'config.scheduler_round_robin': 'Round Robin',
    'config.scheduler_random': 'Random',
    'config.scheduler_least_used': 'Least Used',
    'config.max_retries': 'Max Retries',
    'config.max_retries_tip': 'Maximum retries on request failure',
    'config.backoff_ms': 'Backoff Time (ms)',
    'config.backoff_ms_tip': 'Backoff wait time between retries',
    'config.rpm': 'Requests Per Minute',
    'config.rpm_tip': 'Maximum requests allowed per minute',
    'config.cooldown_ms': 'Cooldown Time (ms)',
    'config.cooldown_ms_tip': 'Account cooldown duration',
    'config.health_enabled': 'Enable Health Check',
    'config.health_enabled_tip': 'Whether to periodically check account status',
    'config.health_interval': 'Check Interval (min)',
    'config.health_interval_tip': 'Health check execution interval',
    'config.auto_refresh': 'Auto Refresh Credentials',
    'config.auto_refresh_tip': 'Whether to auto refresh token before expiry',
    'config.refresh_before': 'Refresh Before (sec)',
    'config.refresh_before_tip': 'How many seconds before token expiry to start refresh',
    'config.api_token': 'Credentials API Token',
    'config.api_token_tip': 'Long token string for POST /api/credentials authentication',
    'config.api_keys_title': 'API Keys',
    'config.api_keys_desc': 'Create dedicated API keys for each caller identity',
    'config.api_keys_add': 'Create API Key',
    'config.api_keys_refresh': 'Refresh API Keys',
    'config.api_keys_empty': 'No API keys',
    'config.api_key_id': 'Key ID',
    'config.api_key_identity': 'Identity',
    'config.api_key_key': 'Key',
    'config.api_key_enabled': 'Enabled',
    'config.api_key_created_at': 'Created At',
    'config.api_key_actions': 'Actions',
    'config.api_key_enable': 'Enable',
    'config.api_key_disable': 'Disable',
    'config.api_key_rotate': 'Rotate',
    'config.api_key_delete': 'Delete',
    'config.api_key_created': 'API key created',
    'config.api_key_rotated': 'API key rotated',
    'config.api_key_deleted': 'API key deleted',
    'config.api_key_create_failed': 'Failed to create API key',
    'config.api_key_update_failed': 'Failed to update API key',
    'config.api_key_delete_failed': 'Failed to delete API key',
    'config.api_key_rotate_failed': 'Failed to rotate API key',
    'config.api_key_plaintext_tip': 'Save the plaintext key now, it will not be shown again',

    // 日志
    'logs.title': 'Request Logs',
    'logs.search_placeholder': 'Search logs...',
    'logs.filter_all': 'All',
    'logs.filter_info': 'Info',
    'logs.filter_warn': 'Warning',
    'logs.filter_error': 'Error',
    'logs.filter_request': 'Request',
    'logs.auto_refresh': 'Auto Refresh',
    'logs.auto_refresh_stop': 'Stop Refresh',
    'logs.manual_refresh': 'Refresh',
    'logs.clear': 'Clear',
    'logs.clear_confirm': 'Are you sure you want to clear all logs? This action cannot be undone.',
    'logs.clear_title': 'Clear Logs',
    'logs.clear_success': 'Logs cleared',
    'logs.clear_failed': 'Failed to clear logs',
    'logs.load_more': 'Load More',
    'logs.no_logs': 'No logs',
    'logs.load_failed': 'Failed to load logs',
    'logs.copied': 'Copied to clipboard',
    'logs.copy_failed': 'Copy failed',
    'logs.copy': 'Copy',

    // Abuse
    'abuse.users_title': 'Risk Users',
    'abuse.events_title': 'Risk Events',
    'abuse.rules_title': 'Rules Configuration',
    'abuse.save_rules': 'Save Rules',
    'abuse.filter_all': 'All Levels',
    'abuse.level_low': 'Low',
    'abuse.level_medium': 'Medium',
    'abuse.level_high': 'High',
    'abuse.level_critical': 'Critical',
    'abuse.sort_score_desc': 'Score Desc',
    'abuse.sort_score_asc': 'Score Asc',
    'abuse.sort_updated_desc': 'Recently Evaluated',
    'abuse.search_placeholder': 'Search identity/user',
    'abuse.refresh': 'Refresh',
    'abuse.col_seq_id': 'ID',
    'abuse.col_username': 'Username',
    'abuse.col_requests': 'Requests',
    'abuse.col_input_tokens': 'Input Tokens',
    'abuse.col_output_tokens': 'Output Tokens',
    'abuse.col_cached_tokens': 'Cached Tokens',
    'abuse.col_last_active': 'Last Active',
    'abuse.history_title': 'Request History',
    'abuse.col_time': 'Time',
    'abuse.col_model': 'Model',
    'abuse.col_status': 'Status',
    'abuse.col_latency': 'Latency(ms)',
    'abuse.col_ip': 'IP',
    'abuse.load_more': 'Load More',
    'abuse.no_history': 'No Records',
    'abuse.th_identity': 'Identity',
    'abuse.th_score': 'Score',
    'abuse.th_level': 'Level',
    'abuse.th_action': 'Action',
    'abuse.th_reasons': 'Rules Hit',
    'abuse.th_time': 'Last Evaluated',
    'abuse.th_actions': 'Actions',
    'abuse.th_rule': 'Rule',
    'abuse.th_value': 'Value',
    'abuse.th_threshold': 'Threshold',
    'abuse.view_detail': 'Detail',
    'abuse.card_total_users': 'Total Users',
    'abuse.card_risk_users': 'Risk Identities',
    'abuse.card_critical_users': 'Critical Risk',
    'abuse.card_suspend_users': 'Suspended',
    'abuse.card_today_events': 'Events Today',
    'abuse.users_empty': 'No risk users',
    'abuse.events_empty': 'No risk events',
    'abuse.detail_title': 'Risk Detail',
    'abuse.detail_rules': 'Matched Rules',
    'abuse.detail_timeline': 'Event Timeline',
    'abuse.rules_empty': 'No matched rules',
    'abuse.load_failed': 'Failed to load abuse data',
    'abuse.invalid_rules_json': 'Invalid rules JSON',
    'abuse.rules_saved': 'Rules saved',
    'abuse.save_failed': 'Save failed',
    'abuse.confirm_action': 'Confirm action: {action}?',
    'abuse.action_done': 'Action applied',
    'abuse.action_failed': 'Action failed',
    'abuse.action_observe': 'Observe',
    'abuse.action_throttle': 'Throttle',
    'abuse.action_challenge': 'Challenge',
    'abuse.action_suspend': 'Suspend',
    'abuse.action_restore': 'Restore',

    // 账号登录
    'accounts.browser_login': 'Login',
    'accounts.browser_login_title': 'Account Login',
    'accounts.browser_login_desc': 'Enter ChatGPT email and password, server will auto login and extract credentials',
    'accounts.browser_login_email': 'Email',
    'accounts.browser_login_email_placeholder': 'Enter ChatGPT email',
    'accounts.browser_login_password': 'Password',
    'accounts.browser_login_password_placeholder': 'Enter password',
    'accounts.browser_login_submit': 'Login & Extract Credentials',
    'accounts.browser_login_submitting': 'Logging in...',
    'accounts.browser_login_success': 'Login successful, credentials added',
    'accounts.browser_login_failed': 'Login failed',
    'accounts.browser_login_empty': 'Please enter email and password',

    // 主题
    'theme.toggle': 'Toggle Theme',
    'theme.light': 'Light Mode',
    'theme.dark': 'Dark Mode',

    // 通用
    'common.confirm': 'Confirm',
    'common.cancel': 'Cancel',
    'common.loading': 'Loading...',
    'common.save': 'Save',
    'common.delete': 'Delete',
    'common.edit': 'Edit',
    'common.close': 'Close',
    'common.yes': 'Yes',
    'common.no': 'No',
    'common.enabled': 'Enabled',
    'common.disabled': 'Disabled',
    'common.logout': 'Logout',
    'common.version_label': 'codex2api',
    'common.success': 'Success',
    'common.error': 'Error',
    'common.warning': 'Warning',
    'common.email_show': 'Show emails',
    'common.email_hide': 'Hide emails',
    'common.admin_password': 'Admin Password',
    'common.enter_admin_password': 'Enter admin password',
    'common.password_required': 'Admin password is required',

    // 时间
    'time.days': 'd',
    'time.hours': 'h',
    'time.minutes': 'min',
    'time.just_now': 'just now',
    'time.seconds_ago': '{n}s ago',
    'time.minutes_ago': '{n}m ago',
    'time.hours_ago': '{n}h ago',
    'time.days_ago': '{n}d ago',
    'time.ms': 'ms',
  },
};

var _currentLang = localStorage.getItem('codex2api_lang') || 'zh';

/**
 * 设置当前语言并重新应用 i18n
 */
function setLang(lang) {
  _currentLang = lang;
  localStorage.setItem('codex2api_lang', lang);
  applyI18n();
}

/**
 * 获取当前语言
 */
function getLang() {
  return _currentLang;
}

/**
 * 获取 i18n 文本
 */
function t(key, params) {
  var dict = _i18n[_currentLang] || _i18n['zh'];
  var str = dict[key] || _i18n['zh'][key] || key;
  if (params) {
    for (var k in params) {
      if (params.hasOwnProperty(k)) {
        str = str.replace(new RegExp('\\{' + k + '\\}', 'g'), params[k]);
      }
    }
  }
  return str;
}

/**
 * 扫描 DOM 中 data-i18n 属性并填充文本
 */
function applyI18n(root) {
  var container = root || document;
  var elements = container.querySelectorAll('[data-i18n]');
  for (var i = 0; i < elements.length; i++) {
    var el = elements[i];
    var key = el.getAttribute('data-i18n');
    if (key) {
      el.textContent = t(key);
    }
  }
  // placeholder
  var placeholders = container.querySelectorAll('[data-i18n-placeholder]');
  for (var j = 0; j < placeholders.length; j++) {
    var el2 = placeholders[j];
    var key2 = el2.getAttribute('data-i18n-placeholder');
    if (key2) {
      el2.setAttribute('placeholder', t(key2));
    }
  }
  // title
  var titles = container.querySelectorAll('[data-i18n-title]');
  for (var k = 0; k < titles.length; k++) {
    var el3 = titles[k];
    var key3 = el3.getAttribute('data-i18n-title');
    if (key3) {
      el3.setAttribute('title', t(key3));
    }
  }
}

/* ============ 全局日期范围 ============ */

var _dateRangeStorageKey = 'codex2api_date_range';
var _dateRange = { mode: 'preset', preset: 'today', from: null, to: null };

function _todayStr() {
  var d = new Date();
  return d.getFullYear() + '-' + _pad2(d.getMonth() + 1) + '-' + _pad2(d.getDate());
}

function _dateShift(base, deltaDays) {
  var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(base);
  if (!m) return base;
  var ts = Date.UTC(parseInt(m[1], 10), parseInt(m[2], 10) - 1, parseInt(m[3], 10));
  var d = new Date(ts + deltaDays * 86400000);
  return d.getUTCFullYear() + '-' + _pad2(d.getUTCMonth() + 1) + '-' + _pad2(d.getUTCDate());
}

function _normalizeDateRange(opts) {
  opts = opts || {};
  var mode = opts.mode || 'preset';
  var preset = opts.preset || 'today';
  var from = opts.from || null;
  var to = opts.to || null;

  if (mode === 'total' || preset === 'total') {
    return { mode: 'total', preset: 'total', from: null, to: null };
  }

  if (mode === 'custom') {
    if (!from || !to) {
      return { mode: 'preset', preset: 'today', from: null, to: null };
    }
    if (from > to) {
      var tmp = from;
      from = to;
      to = tmp;
    }
    return { mode: 'custom', preset: null, from: from, to: to };
  }

  var validPresets = ['last1h', 'last3h', 'last6h', 'last12h', 'today', 'yesterday', 'last7', 'last30'];
  if (validPresets.indexOf(preset) < 0) preset = 'today';
  return { mode: 'preset', preset: preset, from: null, to: null };
}

function _writeDateRange() {
  try {
    localStorage.setItem(_dateRangeStorageKey, JSON.stringify(_dateRange));
  } catch (_) {
    // 忽略存储失败
  }
}

function getDateRange() {
  return {
    mode: _dateRange.mode,
    preset: _dateRange.preset,
    from: _dateRange.from,
    to: _dateRange.to,
  };
}

function setDateRange(opts) {
  _dateRange = _normalizeDateRange(opts);
  _writeDateRange();
  window.dispatchEvent(new CustomEvent('daterange-change', { detail: getDateRange() }));
}

function restoreDateRange() {
  var parsed = null;
  try {
    var raw = localStorage.getItem(_dateRangeStorageKey);
    if (raw) parsed = JSON.parse(raw);
  } catch (_) {
    // 忽略损坏数据
  }
  _dateRange = _normalizeDateRange(parsed || _dateRange);
  _writeDateRange();
  return getDateRange();
}

function dateRangeToQuery(range) {
  var r = _normalizeDateRange(range || _dateRange);
  if (r.mode === 'total') {
    return { total: 'true' };
  }
  if (r.mode === 'custom') {
    return { from: r.from, to: r.to };
  }
  if (r.preset === 'last1h') {
    return { hours: '1' };
  }
  if (r.preset === 'last3h') {
    return { hours: '3' };
  }
  if (r.preset === 'last6h') {
    return { hours: '6' };
  }
  if (r.preset === 'last12h') {
    return { hours: '12' };
  }
  var today = _todayStr();
  if (r.preset === 'today') {
    return { hours: '24' };
  }
  if (r.preset === 'yesterday') {
    var yesterday = _dateShift(today, -1);
    return { from: yesterday, to: yesterday };
  }
  if (r.preset === 'last7') {
    return { from: _dateShift(today, -6), to: today };
  }
  if (r.preset === 'last30') {
    return { from: _dateShift(today, -29), to: today };
  }
  return { hours: '24' };
}

function dateRangeLabel(range) {
  var r = _normalizeDateRange(range || _dateRange);
  if (r.mode === 'total') return t('stats.total');
  if (r.mode === 'custom') {
    if (r.from === r.to) return r.from || '-';
    return (r.from || '-') + ' ~ ' + (r.to || '-');
  }
  if (r.preset === 'last1h') return t('date.last1h');
  if (r.preset === 'last3h') return t('date.last3h');
  if (r.preset === 'last6h') return t('date.last6h');
  if (r.preset === 'last12h') return t('date.last12h');
  if (r.preset === 'today') return t('stats.last_24h');
  if (r.preset === 'yesterday') return t('stats.yesterday');
  if (r.preset === 'last7') return t('stats.last_7_days');
  if (r.preset === 'last30') return t('stats.last_30_days');
  return t('stats.last_24h');
}

function buildQueryString(params) {
  if (!params) return '';
  var keys = Object.keys(params);
  var arr = [];
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var val = params[key];
    if (val === null || val === undefined || val === '') continue;
    arr.push(encodeURIComponent(key) + '=' + encodeURIComponent(String(val)));
  }
  return arr.length ? ('?' + arr.join('&')) : '';
}

/* ============ API 封装 ============ */

function _extractApiErrorMessage(data) {
  if (!data) return t('common.loading');
  if (typeof data.error === 'string' && data.error) return data.error;
  if (data.error && typeof data.error === 'object') {
    if (typeof data.error.message === 'string' && data.error.message) return data.error.message;
    if (typeof data.error.code === 'string' && data.error.code) return data.error.code;
    if (typeof data.error.type === 'string' && data.error.type) return data.error.type;
  }
  if (typeof data.message === 'string' && data.message) return data.message;
  return t('common.loading');
}

/**
 * 发起 API 请求，自动带 token，自动处理 401
 * @param {{skipLogoutOn401?: boolean}=} requestOptions
 */
function api(method, path, body, requestOptions) {
  var reqOpts = requestOptions || {};
  var headers = {
    'Content-Type': 'application/json',
  };
  var token = localStorage.getItem('codex2api_token');
  if (token) {
    headers['Authorization'] = 'Bearer ' + token;
  }
  var opts = {
    method: method,
    headers: headers,
  };
  if (body !== undefined) {
    opts.body = JSON.stringify(body);
  }
  return fetch('/admin/api' + path, opts).then(function (res) {
    if (res.status === 401) {
      if (!reqOpts.skipLogoutOn401 && typeof App !== 'undefined' && App.logout) {
        App.logout();
      }
      throw new Error(t('login.expired'));
    }
    return res.json().then(function (data) {
      if (!res.ok) {
        throw new Error(_extractApiErrorMessage(data));
      }
      return data;
    });
  });
}

/**
 * 原始 fetch 封装（用于文件下载等非 JSON 场景）
 */
function apiRaw(method, path, opts) {
  var token = localStorage.getItem('codex2api_token');
  var headers = (opts && opts.headers) ? opts.headers : {};
  if (token) {
    headers['Authorization'] = 'Bearer ' + token;
  }
  var fetchOpts = Object.assign({}, opts || {}, {
    method: method,
    headers: headers,
  });
  return fetch('/admin/api' + path, fetchOpts).then(function (res) {
    if (res.status === 401) {
      if (typeof App !== 'undefined' && App.logout) {
        App.logout();
      }
      throw new Error(t('login.expired'));
    }
    return res;
  });
}

/* ============ HTML 转义 ============ */

/**
 * HTML 转义 — 防止 XSS 注入
 */
function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/**
 * JS 字符串转义（用于极端场景）
 */
function escapeJs(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}

/**
 * 正则特殊字符转义
 */
function escapeRegExp(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/* ============ 时间格式化 ============ */

function _pad2(n) {
  return n < 10 ? '0' + n : '' + n;
}

/**
 * Unix 时间戳（秒）→ 可读字符串
 */
function formatTime(ts) {
  if (!ts || ts <= 0) return '-';
  var d = new Date(ts * 1000);
  var now = Date.now();
  var diff = d.getTime() - now;

  var dateStr = d.getFullYear() + '-' +
    _pad2(d.getMonth() + 1) + '-' +
    _pad2(d.getDate()) + ' ' +
    _pad2(d.getHours()) + ':' +
    _pad2(d.getMinutes());

  if (diff < 0) {
    return dateStr + ' (' + t('status.expired') + ')';
  }
  if (diff < 3600000) {
    return dateStr + ' (' + Math.floor(diff / 60000) + t('time.minutes') + ')';
  }
  if (diff < 86400000) {
    return dateStr + ' (' + Math.floor(diff / 3600000) + t('time.hours') + ')';
  }
  return dateStr;
}

/**
 * 格式化运行时间（秒 → "X天 X小时 X分钟"）
 */
function formatUptime(sec) {
  if (!sec || sec <= 0) return '-';
  var seconds = Math.floor(sec);
  var days = Math.floor(seconds / 86400);
  var hours = Math.floor((seconds % 86400) / 3600);
  var mins = Math.floor((seconds % 3600) / 60);
  var parts = [];
  if (days > 0) parts.push(days + ' ' + t('time.days'));
  if (hours > 0) parts.push(hours + ' ' + t('time.hours'));
  parts.push(mins + ' ' + t('time.minutes'));
  return parts.join(' ');
}

/**
 * 小时数 → 可读寿命字符串
 * 规则：<1h 显示分钟，1-48h 显示小时，>48h 显示天
 */
function formatLifespanHours(hours) {
  var h = Number(hours);
  if (!Number.isFinite(h) || h < 0) return '-';
  if (h < 1) {
    return (Math.round(h * 600) / 10) + t('time.minutes');
  }
  if (h <= 48) {
    return (Math.round(h * 10) / 10) + t('time.hours');
  }
  return (Math.round((h / 24) * 10) / 10) + t('time.days');
}

/**
 * ISO 时间戳 → 本地可读字符串
 */
function formatDateTime(isoStr) {
  if (!isoStr) return '-';
  var d = new Date(isoStr);
  return d.getFullYear() + '-' +
    _pad2(d.getMonth() + 1) + '-' +
    _pad2(d.getDate()) + ' ' +
    _pad2(d.getHours()) + ':' +
    _pad2(d.getMinutes()) + ':' +
    _pad2(d.getSeconds());
}

/**
 * 相对时间（"3分钟前"、"1小时前"等）
 */
function formatRelativeTime(ts) {
  if (!ts) return '-';
  var d = (typeof ts === 'number') ? new Date(ts * 1000) : new Date(ts);
  var diff = Date.now() - d.getTime();
  if (diff < 0) return t('time.just_now');
  if (diff < 60000) return t('time.seconds_ago', { n: Math.floor(diff / 1000) });
  if (diff < 3600000) return t('time.minutes_ago', { n: Math.floor(diff / 60000) });
  if (diff < 86400000) return t('time.hours_ago', { n: Math.floor(diff / 3600000) });
  return t('time.days_ago', { n: Math.floor(diff / 86400000) });
}

/* ============ 防抖 ============ */

function debounce(fn, ms) {
  var timer = null;
  return function () {
    var self = this;
    var args = arguments;
    if (timer) clearTimeout(timer);
    timer = setTimeout(function () {
      fn.apply(self, args);
    }, ms);
  };
}

/* ============ 邮箱脱敏 ============ */

var _emailMasked = true;

function isEmailMasked() {
  return _emailMasked;
}

function toggleEmailMask() {
  _emailMasked = !_emailMasked;
  // 更新眼睛图标
  var btn = document.getElementById('emailToggleBtn');
  if (btn) {
    btn.setAttribute('title', t(_emailMasked ? 'common.email_show' : 'common.email_hide'));
    btn.innerHTML = _emailMasked
      ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>'
      : '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>';
  }
  // 刷新当前页面的邮箱显示
  var activePage = document.querySelector('.page.active');
  if (activePage) {
    var id = activePage.id;
    if (id === 'pageAccounts' && typeof Accounts !== 'undefined') {
      Accounts.render();
    } else if (id === 'pageStatistics' && typeof Statistics !== 'undefined') {
      Statistics.init(true);
    }
  }
}

function maskEmail(email) {
  if (!email) return '-';
  var at = email.indexOf('@');
  if (at <= 1) return email;
  var local = email.substring(0, at);
  var domain = email.substring(at);
  if (local.length <= 2) {
    return local.charAt(0) + '***' + domain;
  }
  return local.charAt(0) + '***' + local.charAt(local.length - 1) + domain;
}

function displayEmail(email) {
  if (!email) return '-';
  return _emailMasked ? maskEmail(email) : email;
}
