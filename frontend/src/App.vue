<template>
  <div class="app">
    <header>
      <h1>Distributed File Metadata Manager</h1>
      <p class="subtitle">Interactive Testing UI - etcd + Go + gRPC</p>
    </header>

    <main>
      <!-- Server Selection -->
      <section class="servers">
        <h2>File Servers</h2>
        <div class="server-cards">
          <div
            v-for="server in servers"
            :key="server.id"
            :class="['server-card', { active: selectedServer === server.id }]"
            @click="selectServer(server.id)"
          >
            <div class="server-status" :class="server.status"></div>
            <div class="server-info">
              <h3>{{ server.name }}</h3>
              <p>HTTP: {{ server.httpPort }}</p>
              <p class="status-text">{{ server.status }}</p>
            </div>
          </div>
        </div>
      </section>

      <!-- Operations Panel -->
      <section class="operations">
        <h2>Operations (via {{ servers.find(s => s.id === selectedServer)?.name }})</h2>

        <div class="tabs">
          <button
            v-for="tab in tabs"
            :key="tab"
            :class="{ active: activeTab === tab }"
            @click="activeTab = tab"
          >
            {{ tab }}
          </button>
        </div>

        <!-- Create File -->
        <div v-if="activeTab === 'Create'" class="tab-content">
          <div class="form-group">
            <label>File ID</label>
            <input v-model="createForm.fileId" placeholder="e.g., document-001" />
          </div>
          <div class="form-group">
            <label>Owner</label>
            <input v-model="createForm.owner" placeholder="e.g., user@example.com" />
          </div>
          <div class="form-group">
            <label>Attributes (JSON)</label>
            <textarea v-model="createForm.attributes" placeholder='{"type": "pdf", "size": "1024"}'></textarea>
          </div>
          <button class="btn primary" @click="createFile">Create File</button>
        </div>

        <!-- Lock Operations -->
        <div v-if="activeTab === 'Lock'" class="tab-content">
          <div class="form-group">
            <label>File ID</label>
            <input v-model="lockForm.fileId" placeholder="e.g., document-001" />
          </div>
          <div class="button-group">
            <button class="btn success" @click="acquireLock">Acquire Lock</button>
            <button class="btn danger" @click="releaseLock">Release Lock</button>
            <button class="btn secondary" @click="checkLockStatus">Check Status</button>
          </div>
          <div v-if="lockStatus" class="lock-status">
            <h4>Lock Status</h4>
            <pre>{{ JSON.stringify(lockStatus, null, 2) }}</pre>
          </div>
        </div>

        <!-- Update File -->
        <div v-if="activeTab === 'Update'" class="tab-content">
          <div class="form-group">
            <label>File ID</label>
            <input v-model="updateForm.fileId" placeholder="e.g., document-001" />
          </div>
          <div class="form-group">
            <label>New Attributes (JSON)</label>
            <textarea v-model="updateForm.attributes" placeholder='{"status": "reviewed"}'></textarea>
          </div>
          <button class="btn primary" @click="updateFile">Update File</button>
          <p class="hint">Note: You must acquire a lock before updating</p>
        </div>

        <!-- List/View Files -->
        <div v-if="activeTab === 'List'" class="tab-content">
          <button class="btn primary" @click="listFiles">Refresh Files</button>
          <div class="files-grid">
            <div v-for="file in files" :key="file.file_id" class="file-card">
              <h4>{{ file.file_id }}</h4>
              <p><strong>Owner:</strong> {{ file.owner }}</p>
              <p><strong>Version:</strong> {{ file.version }}</p>
              <p><strong>Updated:</strong> {{ formatDate(file.updated_at) }}</p>
              <p><strong>Modified by:</strong> {{ file.last_modified_by }}</p>
              <details>
                <summary>Attributes</summary>
                <pre>{{ JSON.stringify(file.attributes, null, 2) }}</pre>
              </details>
              <button class="btn small danger" @click="deleteFile(file.file_id)">Delete</button>
            </div>
          </div>
        </div>

        <!-- Simulations Tab -->
        <div v-if="activeTab === 'Simulations'" class="tab-content simulations-tab">
          <div class="sim-header">
            <h3>Race Condition Simulations</h3>
            <p>Watch distributed locking in action with real-time animations</p>
          </div>

          <div class="sim-buttons">
            <button
              class="sim-btn"
              @click="runSimulation(1)"
              :disabled="simulationRunning"
            >
              <span class="sim-icon">1</span>
              <span class="sim-label">Basic Lock Contention</span>
              <span class="sim-desc">Two servers compete for same lock</span>
            </button>
            <button
              class="sim-btn"
              @click="runSimulation(2)"
              :disabled="simulationRunning"
            >
              <span class="sim-icon">2</span>
              <span class="sim-label">Three-Way Race</span>
              <span class="sim-desc">Three servers race simultaneously</span>
            </button>
            <button
              class="sim-btn"
              @click="runSimulation(3)"
              :disabled="simulationRunning"
            >
              <span class="sim-icon">3</span>
              <span class="sim-label">Lock Timeout</span>
              <span class="sim-desc">Long operation causes timeout</span>
            </button>
          </div>

          <!-- Simulation Visualization -->
          <div v-if="simulationEvents.length > 0 || simulationRunning" class="sim-visualization">
            <div class="sim-status-bar">
              <span v-if="simulationRunning" class="running">
                <span class="pulse"></span> Simulation Running...
              </span>
              <span v-else class="completed">Simulation Complete</span>
            </div>

            <!-- Server States -->
            <div class="sim-servers">
              <div
                v-for="srv in simServerStates"
                :key="srv.id"
                :class="['sim-server', srv.state]"
              >
                <div class="srv-header">
                  <div class="srv-indicator" :class="srv.state"></div>
                  <span class="srv-name">{{ srv.name }}</span>
                </div>
                <div class="srv-status">{{ srv.statusText }}</div>
                <div v-if="srv.hasLock" class="srv-lock">
                  <span class="lock-icon">LOCK</span>
                </div>
              </div>
            </div>

            <!-- File State -->
            <div class="sim-file" v-if="simFileState.id">
              <div class="file-header">
                <span class="file-icon">FILE</span>
                <span class="file-name">{{ simFileState.id }}</span>
              </div>
              <div class="file-version">Version: {{ simFileState.version }}</div>
              <div class="file-lock-status" :class="{ locked: simFileState.locked }">
                {{ simFileState.locked ? `Locked by ${simFileState.lockHolder}` : 'Unlocked' }}
              </div>
            </div>

            <!-- Event Timeline -->
            <div class="sim-timeline">
              <h4>Event Timeline</h4>
              <div class="timeline-events" ref="timelineRef">
                <div
                  v-for="(event, idx) in simulationEvents"
                  :key="idx"
                  :class="['timeline-event', event.status, getServerClass(event.server)]"
                >
                  <div class="event-time">{{ event.timestamp }}</div>
                  <div class="event-server">{{ event.server }}</div>
                  <div class="event-action">
                    <span :class="['action-badge', event.action]">{{ event.action }}</span>
                  </div>
                  <div class="event-message">{{ event.message }}</div>
                  <div v-if="event.details" class="event-details">{{ event.details }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Activity Log -->
      <section class="log">
        <h2>Activity Log</h2>
        <div class="log-entries">
          <div v-for="(entry, idx) in activityLog" :key="idx" :class="['log-entry', entry.type]">
            <span class="timestamp">{{ entry.time }}</span>
            <span class="server">{{ entry.server }}</span>
            <span class="message">{{ entry.message }}</span>
          </div>
        </div>
      </section>
    </main>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'App',
  data() {
    return {
      selectedServer: 'server1',
      activeTab: 'Simulations',
      tabs: ['Create', 'Lock', 'Update', 'List', 'Simulations'],
      servers: [
        { id: 'server1', name: 'Server 1', httpPort: 8081, status: 'checking' },
        { id: 'server2', name: 'Server 2', httpPort: 8082, status: 'checking' },
        { id: 'server3', name: 'Server 3', httpPort: 8083, status: 'checking' }
      ],
      createForm: { fileId: '', owner: '', attributes: '{}' },
      lockForm: { fileId: '' },
      updateForm: { fileId: '', attributes: '{}' },
      lockStatus: null,
      files: [],
      activityLog: [],
      // Simulation state
      simulationRunning: false,
      simulationEvents: [],
      simServerStates: [
        { id: 'server-1', name: 'Server 1', state: 'idle', statusText: 'Idle', hasLock: false },
        { id: 'server-2', name: 'Server 2', state: 'idle', statusText: 'Idle', hasLock: false },
        { id: 'server-3', name: 'Server 3', state: 'idle', statusText: 'Idle', hasLock: false }
      ],
      simFileState: { id: '', version: 0, locked: false, lockHolder: '' }
    }
  },
  computed: {
    apiBase() {
      const server = this.servers.find(s => s.id === this.selectedServer)
      return `http://localhost:${server.httpPort}/api`
    }
  },
  mounted() {
    this.checkServerStatus()
    this.listFiles()
    setInterval(() => this.checkServerStatus(), 5000)
  },
  methods: {
    selectServer(id) {
      this.selectedServer = id
      this.log('info', `Switched to ${id}`)
    },

    async checkServerStatus() {
      for (const server of this.servers) {
        try {
          await axios.get(`http://localhost:${server.httpPort}/health`, { timeout: 2000 })
          server.status = 'online'
        } catch {
          server.status = 'offline'
        }
      }
    },

    getServerClass(serverName) {
      if (serverName === 'server-1') return 'srv1'
      if (serverName === 'server-2') return 'srv2'
      if (serverName === 'server-3') return 'srv3'
      return 'system'
    },

    resetSimulationState() {
      this.simulationEvents = []
      this.simServerStates = [
        { id: 'server-1', name: 'Server 1', state: 'idle', statusText: 'Idle', hasLock: false },
        { id: 'server-2', name: 'Server 2', state: 'idle', statusText: 'Idle', hasLock: false },
        { id: 'server-3', name: 'Server 3', state: 'idle', statusText: 'Idle', hasLock: false }
      ]
      this.simFileState = { id: '', version: 0, locked: false, lockHolder: '' }
    },

    updateSimState(event) {
      // Update server states based on event
      const serverMap = {
        'server-1': 0,
        'server-2': 1,
        'server-3': 2
      }

      const serverIdx = serverMap[event.server]
      if (serverIdx !== undefined) {
        const srv = this.simServerStates[serverIdx]

        switch (event.action) {
          case 'lock':
            if (event.status === 'pending') {
              srv.state = 'locking'
              srv.statusText = 'Acquiring lock...'
            } else if (event.status === 'waiting') {
              srv.state = 'waiting'
              srv.statusText = 'Waiting for lock...'
            } else if (event.status === 'success') {
              srv.state = 'locked'
              srv.statusText = 'Lock acquired!'
              srv.hasLock = true
              this.simFileState.locked = true
              this.simFileState.lockHolder = srv.name
            } else if (event.status === 'failed') {
              srv.state = 'failed'
              srv.statusText = 'Lock failed'
            }
            break
          case 'unlock':
            if (event.status === 'success') {
              srv.state = 'idle'
              srv.statusText = 'Lock released'
              srv.hasLock = false
              this.simFileState.locked = false
              this.simFileState.lockHolder = ''
            }
            break
          case 'update':
            if (event.status === 'pending') {
              srv.state = 'updating'
              srv.statusText = 'Updating file...'
            } else if (event.status === 'success') {
              srv.statusText = 'Updated!'
              this.simFileState.version++
            }
            break
          case 'create':
            if (event.status === 'success') {
              this.simFileState.version = 1
            }
            break
          case 'work':
            srv.state = 'working'
            srv.statusText = event.message
            break
        }
      }

      // Update file ID
      if (event.file_id) {
        this.simFileState.id = event.file_id
      }
    },

    runSimulation(simNumber) {
      this.resetSimulationState()
      this.simulationRunning = true
      this.log('info', `Starting Simulation ${simNumber}`)

      const eventSource = new EventSource(`http://localhost:8081/api/simulation/${simNumber}`)

      eventSource.onmessage = (e) => {
        try {
          const event = JSON.parse(e.data)

          if (event.action === 'end') {
            eventSource.close()
            this.simulationRunning = false
            this.log('success', `Simulation ${simNumber} completed`)
            return
          }

          this.simulationEvents.push(event)
          this.updateSimState(event)

          // Auto-scroll timeline
          this.$nextTick(() => {
            const timeline = this.$refs.timelineRef
            if (timeline) {
              timeline.scrollTop = timeline.scrollHeight
            }
          })
        } catch (err) {
          console.error('Failed to parse event:', err)
        }
      }

      eventSource.onerror = () => {
        eventSource.close()
        this.simulationRunning = false
        this.log('error', 'Simulation connection closed')
      }
    },

    async createFile() {
      try {
        let attrs = {}
        try { attrs = JSON.parse(this.createForm.attributes) } catch {}

        const res = await axios.post(`${this.apiBase}/files`, {
          file_id: this.createForm.fileId,
          owner: this.createForm.owner,
          attributes: attrs
        })

        if (res.data.success) {
          this.log('success', `Created file: ${this.createForm.fileId}`)
          this.listFiles()
        } else {
          this.log('error', res.data.message)
        }
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async acquireLock() {
      try {
        const res = await axios.post(`${this.apiBase}/locks/${this.lockForm.fileId}`)
        if (res.data.success) {
          this.log('success', `Lock acquired for: ${this.lockForm.fileId}`)
          this.lockStatus = res.data.lock_info
        } else {
          this.log('error', res.data.message)
        }
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async releaseLock() {
      try {
        const res = await axios.delete(`${this.apiBase}/locks/${this.lockForm.fileId}`)
        if (res.data.success) {
          this.log('success', `Lock released for: ${this.lockForm.fileId}`)
          this.lockStatus = null
        } else {
          this.log('error', res.data.message)
        }
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async checkLockStatus() {
      try {
        const res = await axios.get(`${this.apiBase}/locks/status/${this.lockForm.fileId}`)
        this.lockStatus = res.data.lock_info
        this.log('info', `Lock status checked for: ${this.lockForm.fileId}`)
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async updateFile() {
      try {
        let attrs = {}
        try { attrs = JSON.parse(this.updateForm.attributes) } catch {}

        const res = await axios.put(`${this.apiBase}/files/${this.updateForm.fileId}`, {
          attributes: attrs
        })

        if (res.data.success) {
          this.log('success', `Updated file: ${this.updateForm.fileId} (v${res.data.metadata.version})`)
          this.listFiles()
        } else {
          this.log('error', res.data.message)
        }
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async deleteFile(fileId) {
      try {
        const res = await axios.delete(`${this.apiBase}/files/${fileId}`)
        if (res.data.success) {
          this.log('success', `Deleted file: ${fileId}`)
          this.listFiles()
        } else {
          this.log('error', res.data.message)
        }
      } catch (err) {
        this.log('error', err.message)
      }
    },

    async listFiles() {
      try {
        const res = await axios.get(`${this.apiBase}/files`)
        if (res.data.success) {
          this.files = res.data.files || []
        }
      } catch (err) {
        console.error('Failed to list files:', err)
      }
    },

    log(type, message) {
      const server = this.servers.find(s => s.id === this.selectedServer)
      this.activityLog.unshift({
        type,
        server: server?.name || 'System',
        message,
        time: new Date().toLocaleTimeString()
      })
      if (this.activityLog.length > 50) this.activityLog.pop()
    },

    formatDate(dateStr) {
      if (!dateStr) return 'N/A'
      return new Date(dateStr).toLocaleString()
    }
  }
}
</script>

<style>
.app {
  max-width: 1400px;
  margin: 0 auto;
  padding: 20px;
}

header {
  text-align: center;
  margin-bottom: 30px;
}

header h1 {
  color: #00d9ff;
  font-size: 2rem;
}

.subtitle {
  color: #888;
  margin-top: 5px;
}

section {
  background: #16213e;
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 20px;
}

section h2 {
  color: #00d9ff;
  margin-bottom: 15px;
  font-size: 1.2rem;
}

.server-cards {
  display: flex;
  gap: 15px;
}

.server-card {
  flex: 1;
  background: #1a1a2e;
  border: 2px solid #333;
  border-radius: 8px;
  padding: 15px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 15px;
  transition: all 0.3s;
}

.server-card:hover {
  border-color: #00d9ff;
}

.server-card.active {
  border-color: #00d9ff;
  background: #1f2b4d;
}

.server-status {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #666;
}

.server-status.online { background: #00ff88; }
.server-status.offline { background: #ff4444; }
.server-status.checking { background: #ffaa00; }

.server-info h3 {
  font-size: 1rem;
  margin-bottom: 5px;
}

.server-info p {
  font-size: 0.85rem;
  color: #888;
}

.status-text {
  text-transform: capitalize;
}

.tabs {
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.tabs button {
  padding: 8px 20px;
  background: #1a1a2e;
  border: 1px solid #333;
  color: #ccc;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.3s;
}

.tabs button:hover {
  border-color: #00d9ff;
}

.tabs button.active {
  background: #00d9ff;
  color: #000;
  border-color: #00d9ff;
}

.tab-content {
  background: #1a1a2e;
  padding: 20px;
  border-radius: 8px;
}

.form-group {
  margin-bottom: 15px;
}

.form-group label {
  display: block;
  margin-bottom: 5px;
  color: #888;
  font-size: 0.9rem;
}

.form-group input,
.form-group textarea {
  width: 100%;
  padding: 10px;
  background: #0f0f1a;
  border: 1px solid #333;
  border-radius: 6px;
  color: #fff;
  font-size: 0.95rem;
}

.form-group textarea {
  min-height: 80px;
  font-family: monospace;
}

.btn {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.95rem;
  transition: all 0.3s;
}

.btn.primary { background: #00d9ff; color: #000; }
.btn.success { background: #00ff88; color: #000; }
.btn.danger { background: #ff4444; color: #fff; }
.btn.secondary { background: #555; color: #fff; }
.btn.small { padding: 5px 10px; font-size: 0.8rem; }

.btn:hover { opacity: 0.8; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }

.button-group {
  display: flex;
  gap: 10px;
  margin-bottom: 15px;
}

.hint {
  color: #888;
  font-size: 0.85rem;
  margin-top: 10px;
}

.lock-status {
  margin-top: 15px;
  background: #0f0f1a;
  padding: 15px;
  border-radius: 6px;
}

.lock-status h4 {
  margin-bottom: 10px;
  color: #00d9ff;
}

.lock-status pre {
  color: #00ff88;
  font-size: 0.85rem;
}

.files-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 15px;
  margin-top: 15px;
}

.file-card {
  background: #0f0f1a;
  padding: 15px;
  border-radius: 8px;
  border: 1px solid #333;
}

.file-card h4 {
  color: #00d9ff;
  margin-bottom: 10px;
}

.file-card p {
  font-size: 0.85rem;
  color: #aaa;
  margin-bottom: 5px;
}

.file-card details {
  margin-top: 10px;
}

.file-card summary {
  cursor: pointer;
  color: #888;
  font-size: 0.85rem;
}

.file-card pre {
  margin-top: 5px;
  font-size: 0.8rem;
  color: #00ff88;
}

.log {
  max-height: 300px;
  overflow-y: auto;
}

.log-entries {
  background: #0f0f1a;
  border-radius: 8px;
  padding: 10px;
}

.log-entry {
  display: flex;
  gap: 15px;
  padding: 8px;
  border-bottom: 1px solid #222;
  font-size: 0.85rem;
}

.log-entry:last-child {
  border-bottom: none;
}

.log-entry .timestamp {
  color: #666;
  min-width: 80px;
}

.log-entry .server {
  color: #00d9ff;
  min-width: 80px;
}

.log-entry.success .message { color: #00ff88; }
.log-entry.error .message { color: #ff4444; }
.log-entry.info .message { color: #aaa; }

/* Simulation Styles */
.simulations-tab {
  min-height: 400px;
}

.sim-header {
  text-align: center;
  margin-bottom: 25px;
}

.sim-header h3 {
  color: #00d9ff;
  font-size: 1.4rem;
  margin-bottom: 8px;
}

.sim-header p {
  color: #888;
}

.sim-buttons {
  display: flex;
  gap: 15px;
  margin-bottom: 25px;
}

.sim-btn {
  flex: 1;
  background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
  border: 2px solid #333;
  border-radius: 12px;
  padding: 20px;
  cursor: pointer;
  transition: all 0.3s;
  text-align: left;
}

.sim-btn:hover:not(:disabled) {
  border-color: #00d9ff;
  transform: translateY(-2px);
  box-shadow: 0 5px 20px rgba(0, 217, 255, 0.2);
}

.sim-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.sim-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  background: #00d9ff;
  color: #000;
  border-radius: 50%;
  font-size: 1.2rem;
  font-weight: bold;
  margin-bottom: 10px;
}

.sim-label {
  display: block;
  color: #fff;
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 5px;
}

.sim-desc {
  display: block;
  color: #888;
  font-size: 0.85rem;
}

.sim-visualization {
  background: #0f0f1a;
  border-radius: 12px;
  padding: 20px;
}

.sim-status-bar {
  text-align: center;
  margin-bottom: 20px;
  padding: 10px;
  border-radius: 8px;
  background: #1a1a2e;
}

.sim-status-bar .running {
  color: #ffaa00;
  display: inline-flex;
  align-items: center;
  gap: 10px;
}

.sim-status-bar .completed {
  color: #00ff88;
}

.pulse {
  width: 10px;
  height: 10px;
  background: #ffaa00;
  border-radius: 50%;
  animation: pulse 1s infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.5; transform: scale(1.2); }
}

.sim-servers {
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
}

.sim-server {
  flex: 1;
  background: #1a1a2e;
  border: 2px solid #333;
  border-radius: 10px;
  padding: 15px;
  transition: all 0.3s;
  position: relative;
}

.sim-server.idle { border-color: #333; }
.sim-server.locking { border-color: #ffaa00; background: rgba(255, 170, 0, 0.1); }
.sim-server.waiting { border-color: #ff6b6b; background: rgba(255, 107, 107, 0.1); }
.sim-server.locked { border-color: #00ff88; background: rgba(0, 255, 136, 0.1); }
.sim-server.updating { border-color: #00d9ff; background: rgba(0, 217, 255, 0.1); }
.sim-server.working { border-color: #a855f7; background: rgba(168, 85, 247, 0.1); }
.sim-server.failed { border-color: #ff4444; background: rgba(255, 68, 68, 0.1); }

.srv-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}

.srv-indicator {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #666;
  transition: all 0.3s;
}

.srv-indicator.idle { background: #666; }
.srv-indicator.locking { background: #ffaa00; animation: pulse 0.5s infinite; }
.srv-indicator.waiting { background: #ff6b6b; animation: pulse 0.8s infinite; }
.srv-indicator.locked { background: #00ff88; }
.srv-indicator.updating { background: #00d9ff; animation: pulse 0.3s infinite; }
.srv-indicator.working { background: #a855f7; animation: pulse 0.5s infinite; }
.srv-indicator.failed { background: #ff4444; }

.srv-name {
  font-weight: 600;
  color: #fff;
}

.srv-status {
  font-size: 0.85rem;
  color: #aaa;
}

.srv-lock {
  position: absolute;
  top: 10px;
  right: 10px;
}

.lock-icon {
  background: #00ff88;
  color: #000;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 0.7rem;
  font-weight: bold;
  animation: lockPulse 1s infinite;
}

@keyframes lockPulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(0, 255, 136, 0.4); }
  50% { box-shadow: 0 0 0 8px rgba(0, 255, 136, 0); }
}

.sim-file {
  background: #1a1a2e;
  border: 2px solid #333;
  border-radius: 10px;
  padding: 15px;
  margin-bottom: 20px;
  text-align: center;
}

.file-header {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  margin-bottom: 10px;
}

.file-icon {
  background: #00d9ff;
  color: #000;
  padding: 5px 12px;
  border-radius: 4px;
  font-size: 0.8rem;
  font-weight: bold;
}

.file-name {
  font-size: 1.1rem;
  color: #fff;
  font-family: monospace;
}

.file-version {
  color: #888;
  font-size: 0.9rem;
  margin-bottom: 5px;
}

.file-lock-status {
  display: inline-block;
  padding: 5px 15px;
  border-radius: 20px;
  font-size: 0.85rem;
  background: #333;
  color: #888;
}

.file-lock-status.locked {
  background: rgba(0, 255, 136, 0.2);
  color: #00ff88;
  border: 1px solid #00ff88;
}

.sim-timeline {
  background: #1a1a2e;
  border-radius: 10px;
  padding: 15px;
}

.sim-timeline h4 {
  color: #00d9ff;
  margin-bottom: 15px;
}

.timeline-events {
  max-height: 300px;
  overflow-y: auto;
}

.timeline-event {
  display: grid;
  grid-template-columns: 90px 80px 80px 1fr;
  gap: 10px;
  padding: 10px;
  border-radius: 6px;
  margin-bottom: 8px;
  background: #0f0f1a;
  align-items: center;
  animation: slideIn 0.3s ease-out;
}

@keyframes slideIn {
  from { opacity: 0; transform: translateX(-20px); }
  to { opacity: 1; transform: translateX(0); }
}

.timeline-event.srv1 { border-left: 3px solid #00d9ff; }
.timeline-event.srv2 { border-left: 3px solid #00ff88; }
.timeline-event.srv3 { border-left: 3px solid #a855f7; }
.timeline-event.system { border-left: 3px solid #ffaa00; }

.event-time {
  font-family: monospace;
  color: #666;
  font-size: 0.8rem;
}

.event-server {
  font-size: 0.85rem;
  color: #aaa;
}

.event-action {
  text-align: center;
}

.action-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.75rem;
  font-weight: bold;
  text-transform: uppercase;
}

.action-badge.init { background: #333; color: #fff; }
.action-badge.create { background: #00d9ff; color: #000; }
.action-badge.lock { background: #ffaa00; color: #000; }
.action-badge.unlock { background: #00ff88; color: #000; }
.action-badge.update { background: #a855f7; color: #fff; }
.action-badge.work { background: #666; color: #fff; }
.action-badge.complete { background: #00ff88; color: #000; }
.action-badge.error { background: #ff4444; color: #fff; }

.event-message {
  color: #fff;
  font-size: 0.9rem;
}

.timeline-event.success .event-message { color: #00ff88; }
.timeline-event.failed .event-message { color: #ff4444; }
.timeline-event.waiting .event-message { color: #ff6b6b; }
.timeline-event.pending .event-message { color: #ffaa00; }

.event-details {
  grid-column: 1 / -1;
  color: #666;
  font-size: 0.8rem;
  padding-left: 90px;
  margin-top: -5px;
}
</style>
