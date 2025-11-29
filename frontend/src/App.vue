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

        <!-- Race Condition Demo -->
        <div v-if="activeTab === 'Demo'" class="tab-content">
          <h3>Concurrent Update Demo</h3>
          <p>This will attempt to update the same file from all 3 servers simultaneously.</p>
          <div class="form-group">
            <label>File ID</label>
            <input v-model="demoFileId" placeholder="e.g., demo-file" />
          </div>
          <button class="btn primary" @click="runRaceDemo">Run Concurrent Update Demo</button>
          <div v-if="demoResults.length" class="demo-results">
            <h4>Results</h4>
            <div v-for="(result, idx) in demoResults" :key="idx" class="demo-result">
              <span :class="['badge', result.success ? 'success' : 'error']">
                {{ result.server }}
              </span>
              <span>{{ result.message }}</span>
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
      activeTab: 'Create',
      tabs: ['Create', 'Lock', 'Update', 'List', 'Demo'],
      servers: [
        { id: 'server1', name: 'Server 1', httpPort: 8081, status: 'checking' },
        { id: 'server2', name: 'Server 2', httpPort: 8082, status: 'checking' },
        { id: 'server3', name: 'Server 3', httpPort: 8083, status: 'checking' }
      ],
      createForm: { fileId: '', owner: '', attributes: '{}' },
      lockForm: { fileId: '' },
      updateForm: { fileId: '', attributes: '{}' },
      demoFileId: 'demo-file',
      lockStatus: null,
      files: [],
      demoResults: [],
      activityLog: []
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

    async runRaceDemo() {
      this.demoResults = []
      this.log('info', 'Starting concurrent update demo...')

      // First create the demo file if it doesn't exist
      try {
        await axios.post(`http://localhost:8081/api/files`, {
          file_id: this.demoFileId,
          owner: 'demo',
          attributes: { demo: 'true' }
        })
      } catch {}

      // Try to acquire lock and update from all servers simultaneously
      const promises = this.servers.map(async (server) => {
        const base = `http://localhost:${server.httpPort}/api`
        try {
          // Try to acquire lock
          const lockRes = await axios.post(`${base}/locks/${this.demoFileId}`, {}, { timeout: 15000 })
          if (lockRes.data.success) {
            // Update the file
            const updateRes = await axios.put(`${base}/files/${this.demoFileId}`, {
              attributes: { updated_by: server.id, time: new Date().toISOString() }
            })

            // Release lock
            await axios.delete(`${base}/locks/${this.demoFileId}`)

            return {
              server: server.name,
              success: true,
              message: `Lock acquired, updated to v${updateRes.data.metadata?.version}, lock released`
            }
          } else {
            return {
              server: server.name,
              success: false,
              message: lockRes.data.message
            }
          }
        } catch (err) {
          return {
            server: server.name,
            success: false,
            message: err.response?.data?.message || err.message
          }
        }
      })

      this.demoResults = await Promise.all(promises)
      this.listFiles()
      this.log('info', 'Concurrent update demo completed')
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

.demo-results {
  margin-top: 20px;
}

.demo-results h4 {
  margin-bottom: 10px;
  color: #00d9ff;
}

.demo-result {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px;
  background: #0f0f1a;
  border-radius: 6px;
  margin-bottom: 8px;
}

.badge {
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 0.8rem;
  font-weight: bold;
}

.badge.success { background: #00ff88; color: #000; }
.badge.error { background: #ff4444; color: #fff; }

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
</style>
