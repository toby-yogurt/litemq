// LiteMQ Web界面JavaScript

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    initializePage();
    setupEventListeners();
    startAutoRefresh();
});

// 初始化页面
function initializePage() {
    // 检查当前页面
    const path = window.location.pathname;

    if (path === '/' || path === '/dashboard') {
        loadDashboardData();
    } else if (path === '/brokers') {
        loadBrokersData();
    } else if (path === '/topics') {
        loadTopicsData();
    } else if (path === '/consumers') {
        loadConsumersData();
    } else if (path === '/monitoring') {
        loadMonitoringData();
    }
}

// 设置事件监听器
function setupEventListeners() {
    // 全局错误处理
    window.addEventListener('error', function(e) {
        console.error('JavaScript error:', e.error);
        showNotification('发生了一个错误，请查看控制台', 'danger');
    });

    // 网络错误处理
    window.addEventListener('unhandledrejection', function(e) {
        console.error('Unhandled promise rejection:', e.reason);
        showNotification('网络请求失败', 'warning');
    });
}

// 自动刷新
function startAutoRefresh() {
    // 每30秒自动刷新一次数据
    setInterval(function() {
        const path = window.location.pathname;
        if (path === '/' || path === '/dashboard') {
            loadDashboardData();
        }
    }, 30000);
}

// 加载仪表板数据
function loadDashboardData() {
    Promise.all([
        fetchAPI('/api/v1/broker/stats'),
        fetchAPI('/api/v1/nameserver/stats')
    ]).then(([brokerStats, nsStats]) => {
        updateDashboardStats({
            brokers: nsStats.brokers || 0,
            topics: brokerStats.topics || 0,
            consumers: brokerStats.consumers || 0,
            messages: brokerStats.total_messages || 0
        });
    }).catch(error => {
        console.error('Failed to load dashboard data:', error);
    });
}

// 加载Brokers数据
function loadBrokersData() {
    fetchAPI('/api/v1/nameserver/brokers')
        .then(data => {
            updateBrokersTable(data);
        })
        .catch(error => {
            console.error('Failed to load brokers data:', error);
        });
}

// 加载主题数据
function loadTopicsData() {
    fetchAPI('/api/v1/broker/topics')
        .then(data => {
            updateTopicsTable(data);
        })
        .catch(error => {
            console.error('Failed to load topics data:', error);
        });
}

// 加载消费者数据
function loadConsumersData() {
    fetchAPI('/api/v1/broker/consumers')
        .then(data => {
            updateConsumersTable(data);
        })
        .catch(error => {
            console.error('Failed to load consumers data:', error);
        });
}

// 加载监控数据
function loadMonitoringData() {
    Promise.all([
        fetchAPI('/metrics'),
        fetchAPI('/api/v1/broker/stats')
    ]).then(([metrics, stats]) => {
        updateMonitoringData(metrics, stats);
    }).catch(error => {
        console.error('Failed to load monitoring data:', error);
    });
}

// API请求封装
function fetchAPI(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        },
        timeout: 5000
    };

    const finalOptions = Object.assign({}, defaultOptions, options);

    return Promise.race([
        fetch(url, finalOptions),
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Request timeout')), finalOptions.timeout)
        )
    ]).then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    });
}

// 更新仪表板统计
function updateDashboardStats(stats) {
    // 更新统计卡片
    const statElements = {
        'brokers': stats.brokers,
        'topics': stats.topics,
        'consumers': stats.consumers,
        'messages': stats.messages
    };

    Object.keys(statElements).forEach(key => {
        const element = document.querySelector(`[data-stat="${key}"]`);
        if (element) {
            element.textContent = statElements[key];
        }
    });

    // 更新系统状态
    updateSystemStatus(stats);
}

// 更新系统状态
function updateSystemStatus(stats) {
    const statusElements = document.querySelectorAll('.system-status');
    statusElements.forEach(element => {
        const service = element.dataset.service;
        let status = 'unknown';
        let badgeClass = 'secondary';

        if (service === 'nameserver') {
            status = stats.nameserver_running ? '运行中' : '停止';
            badgeClass = stats.nameserver_running ? 'success' : 'danger';
        } else if (service === 'broker') {
            status = stats.broker_running ? '运行中' : '停止';
            badgeClass = stats.broker_running ? 'success' : 'danger';
        }

        element.innerHTML = `<span class="badge bg-${badgeClass}">${status}</span>`;
    });
}

// 更新Brokers表格
function updateBrokersTable(brokers) {
    const tbody = document.querySelector('#brokersTable tbody');
    if (!tbody) return;

    if (!brokers || brokers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-muted">暂无Broker</td></tr>';
        return;
    }

    tbody.innerHTML = brokers.map(broker => `
        <tr>
            <td>${broker.broker_id || 'N/A'}</td>
            <td>${broker.broker_addr || 'N/A'}</td>
            <td>
                <span class="badge bg-${getStatusBadge(broker.status)}">
                    ${broker.status || 'unknown'}
                </span>
            </td>
            <td>${broker.role || 'N/A'}</td>
            <td>${broker.topics ? Object.keys(broker.topics).length : 0}</td>
            <td>0</td>
            <td>${formatTime(broker.last_update)}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="viewBroker('${broker.broker_id}')">
                    查看
                </button>
            </td>
        </tr>
    `).join('');
}

// 更新主题表格
function updateTopicsTable(topics) {
    const tbody = document.querySelector('#topicsTable tbody');
    if (!tbody) return;

    if (!topics || topics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">暂无主题</td></tr>';
        return;
    }

    tbody.innerHTML = topics.map(topic => `
        <tr>
            <td>${topic.name}</td>
            <td>${topic.partitions || 1}</td>
            <td>${topic.messages || 0}</td>
            <td>${topic.consumers || 0}</td>
            <td>${formatTime(topic.last_updated)}</td>
            <td>
                <button class="btn btn-sm btn-outline-info me-1" onclick="viewTopic('${topic.name}')">
                    查看
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="deleteTopic('${topic.name}')">
                    删除
                </button>
            </td>
        </tr>
    `).join('');
}

// 更新消费者表格
function updateConsumersTable(consumerGroups) {
    const tbody = document.querySelector('#consumersTable tbody');
    if (!tbody) return;

    if (!consumerGroups || consumerGroups.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-muted">暂无消费者组</td></tr>';
        return;
    }

    tbody.innerHTML = consumerGroups.map(group => `
        <tr>
            <td>${group.group_name}</td>
            <td>${group.consumers || 0}</td>
            <td>${group.topics ? group.topics.join(', ') : ''}</td>
            <td>
                <span class="badge bg-${group.status === 'active' ? 'success' : 'warning'}">
                    ${group.status || 'unknown'}
                </span>
            </td>
            <td>${group.lag || 0}</td>
            <td>${formatTime(group.last_consume)}</td>
            <td>
                <button class="btn btn-sm btn-outline-info" onclick="viewConsumerGroup('${group.group_name}')">
                    查看
                </button>
            </td>
        </tr>
    `).join('');
}

// 更新监控数据
function updateMonitoringData(metrics, stats) {
    // 更新指标图表
    updateMetricsCharts(metrics);

    // 更新统计摘要
    updateMetricsSummary(stats);
}

// 更新指标图表
function updateMetricsCharts(metrics) {
    // 这里可以集成Chart.js或其他图表库
    console.log('Metrics data:', metrics);
}

// 更新指标摘要
function updateMetricsSummary(stats) {
    const summaryElement = document.querySelector('.metrics-summary');
    if (!summaryElement) return;

    summaryElement.innerHTML = `
        <div class="row">
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title text-primary">${stats.total_messages || 0}</h5>
                        <p class="card-text">总消息数</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title text-success">${stats.active_connections || 0}</h5>
                        <p class="card-text">活跃连接</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title text-info">${stats.topics || 0}</h5>
                        <p class="card-text">主题数量</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title text-warning">${stats.consumers || 0}</h5>
                        <p class="card-text">消费者数量</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// 获取状态徽章样式
function getStatusBadge(status) {
    switch (status) {
        case 'online':
        case 'running':
            return 'success';
        case 'offline':
        case 'stopped':
            return 'danger';
        case 'warning':
        case 'timeout':
            return 'warning';
        default:
            return 'secondary';
    }
}

// 格式化时间
function formatTime(timestamp) {
    if (!timestamp) return '未知';

    if (typeof timestamp === 'string') {
        return timestamp;
    }

    const date = new Date(timestamp);
    return date.toLocaleString();
}

// 显示通知
function showNotification(message, type = 'info') {
    // 创建通知元素
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    // 添加到页面
    document.body.appendChild(notification);

    // 自动移除
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 5000);
}

// 全局函数供模板调用
window.viewBroker = function(brokerId) {
    showNotification(`查看Broker: ${brokerId}`, 'info');
};

window.viewTopic = function(topicName) {
    showNotification(`查看主题: ${topicName}`, 'info');
};

window.viewConsumerGroup = function(groupName) {
    showNotification(`查看消费者组: ${groupName}`, 'info');
};

window.createTopic = function() {
    showNotification('创建主题功能', 'info');
};

window.deleteTopic = function(topicName) {
    if (confirm(`确定要删除主题 "${topicName}" 吗？`)) {
        showNotification(`删除主题: ${topicName}`, 'warning');
    }
};
