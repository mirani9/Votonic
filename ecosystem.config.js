module.exports = {
  apps: [
    {
      name: 'votecore-engine',
      script: './server.js',
      instances: 'max',
      exec_mode: 'cluster',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PORT: 3000,
        DB_FILE: './votecore_prod.db'
      }
    }
  ]
};
