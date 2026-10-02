import WebSocket from 'ws';
import { EventEmitter } from 'events';

const blackListedEvents = [
    "CHANNEL_UNREAD_UPDATE",
    "CONVERSATION_SUMMARY_UPDATE",
    "SESSIONS_REPLACE"
];

const GATEWAY_URL = 'wss://gateway.discord.gg/?v=10&encoding=json';
const statusList = ["online", "idle", "dnd", "invisible", "offline"];

export class voiceClient extends EventEmitter {
    ws = null;
    heartbeatInterval;
    sequenceNumber = null;
    firstLoad = true;
    reconnectAttempts = 0;
    ignoreReconnect = false;
    reconnectTimeout;
    invalidSession = false;
    token;
    guildId;
    channelId;
    selfMute;
    selfDeaf;
    autoReconnect;
    presence;
    user_id = null;

    constructor(config) {
        super();

        if (!config.token) {
            throw new Error('token, guildId, and channelId are required');
        }

        this.token = config.token;
        this.guildId = config?.serverId;
        this.channelId = config?.channelId;
        this.selfMute = config.selfMute ?? true;
        this.selfDeaf = config.selfDeaf ?? true;

        this.autoReconnect = {
            enabled: config.autoReconnect.enabled ?? false,
            delay: (config.autoReconnect.delay ?? 1) * 1000,
            maxRetries: config.autoReconnect?.maxRetries ?? 9999,
        };

        if (config?.presence?.status) {
            this.presence = config.presence;
        }
    }

    connect() {
        if (this.invalidSession) return;

        this.ws = new WebSocket(GATEWAY_URL, {
            skipUTF8Validation: true,
        });

        this.setMaxListeners(5);

        this.ws.on('open', () => {
            this.emit('connected');
            this.emit('debug', '🌐 Connected to Discord Gateway');
        });

        this.ws.on('message', (data) => {
            const payload = JSON.parse(data.toString());
            const { t: eventType, s: seq, op, d } = payload;
            const isBlackListed = blackListedEvents.includes(eventType);

            if (isBlackListed) return;
            if (seq !== null) this.sequenceNumber = seq;

            switch (op) {
                case 10:
                    this.emit('debug', 'Received Hello (op 10)');
                    this.startHeartbeat(d.heartbeat_interval);
                    this.identify();
                    break;

                case 11:
                    this.emit('debug', 'Heartbeat acknowledged');
                    break;

                case 9:
                    this.emit('debug', 'Invalid session. Reconnecting...');
                    this.invalidSession = true;

                    if (this.ws) {
                        this.ws.terminate();
                    }

                    this.cleanup();
                    break;

                case 0:
                    if (eventType === 'READY') {
                        this.emit('ready', {
                            username: d.user.username,
                            discriminator: d.user.discriminator
                        });

                        this.emit(
                            'debug',
                            `🎉 Logged in as ${d.user.username}#${d.user.discriminator}`
                        );

                        this.user_id = d.user.id;
                        this.joinVoiceChannel();
                        this.sendStatusUpdate();
                    } else if (eventType === 'VOICE_STATE_UPDATE') {
                        if (
                            d.user_id === this.user_id &&
                            d.channel_id === this.channelId &&
                            d?.guild_id === this.guildId &&
                            this.firstLoad
                        ) {
                            this.emit('voiceReady');
                            console.log('Voice channel joined successfully');
                            this.emit(
                                'debug',
                                'Successfully joined voice channel'
                            );
                            this.firstLoad = false;
                        } else if (
                            d.user_id === this.user_id &&
                            (
                                this.guildId &&
                                this.channelId &&
                                d?.channel_id !== this.channelId ||
                                d?.guild_id !== this.guildId
                            )
                        ) {
                            if (this.autoReconnect.enabled) {
                                console.log(
                                    'Received VOICE_STATE_UPDATE event, attempting to reconnect'
                                );

                                if (this.ignoreReconnect) {
                                    console.log(
                                        'Already reconnected, ignoring this event'
                                    );
                                    return;
                                }

                                this.reconnectAttempts++;

                                if (
                                    this.reconnectAttempts <
                                    this.autoReconnect.maxRetries
                                ) {
                                    if (this.reconnectTimeout) {
                                        clearTimeout(this.reconnectTimeout);
                                    }

                                    this.emit(
                                        'debug',
                                        `Reconnecting... (${this.reconnectAttempts}/${this.autoReconnect.maxRetries})`
                                    );

                                    this.ignoreReconnect = true;

                                    this.reconnectTimeout = setTimeout(() => {
                                        this.joinVoiceChannel();
                                    }, this.autoReconnect.delay);
                                } else {
                                    this.emit(
                                        'debug',
                                        'Max reconnect attempts reached. Stopping.'
                                    );
                                    this.cleanup();
                                }
                            }
                        }
                    }
                    break;
            }
        });

        this.ws.on('close', () => {
            this.emit('disconnected');
            this.emit('debug', '❌ Disconnected. Reconnecting...');
            this.cleanup();

            if (this.firstLoad) {
                console.log(`Bad token or invalid channelId/guildId`);
                return;
            }

            setTimeout(() => this.connect(), 5000);
        });

        this.ws.on('error', (err) => {
            this.emit('error', err);
            this.emit('debug', `WebSocket error: ${err.message}`);
        });
    }

    startHeartbeat(interval) {
        this.heartbeatInterval = setInterval(() => {
            this.ws?.send(
                JSON.stringify({
                    op: 1,
                    d: this.sequenceNumber
                })
            );

            this.emit('debug', 'Sending heartbeat');
        }, interval);
    }

    identify() {
        const payload = {
            op: 2,
            d: {
                token: this.token,
                intents: 128,
                properties: {
                    os: 'Windows',
                    browser: 'Chrome',
                    device: ''
                },
            }
        };

        this.ws?.send(JSON.stringify(payload));
        this.emit('debug', 'Sending identify payload');
    }

    joinVoiceChannel() {
        if (!this.guildId || !this.channelId) return;

        const voiceStateUpdate = {
            op: 4,
            d: {
                guild_id: this.guildId,
                channel_id: this.channelId,
                self_mute: this.selfMute,
                self_deaf: this.selfDeaf
            }
        };

        this.ws?.send(JSON.stringify(voiceStateUpdate));
        this.emit('debug', '🎤 Sent voice channel join request');

        setTimeout(() => {
            this.ignoreReconnect = false;
        }, 1000);
    }

    cleanup() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }

        this.ws = null;
        this.sequenceNumber = null;
    }

    sendStatusUpdate() {
        const status = this?.presence?.status?.toLowerCase();

        if (!status || !statusList.includes(status)) return;

        const payload = {
            "op": 3,
            "d": {
                status: this.presence.status,
                activities: [],
                since: Math.floor(Date.now() / 1000) - 10,
                afk: true
            }
        };

        this.ws?.send(JSON.stringify(payload));
        this.emit('debug', `Status updated to ${this.presence.status}`);
    }

    disconnect() {
        this.cleanup();
        this.emit('debug', 'Client manually disconnected');
    }
}
```**الرسالة الثانية — المشروع الثاني: Custom-Status**

### اسم الملف: `index.js`

```js
const Discord = require('discord.js-selfbot-v13');

const client = new Discord.Client({
  readyStatus: false,
  checkUpdate: false
});

//environment
require('dotenv').config();

function formatTime() {
  const date = new Date();

  const options = {
    timeZone: 'America/New_York',
    hour12: true,
    hour: 'numeric',
    minute: 'numeric'
  };

  return new Intl.DateTimeFormat('en-US', options).format(date);
}

const express = require("express");
const app = express();

var listener = app.listen(process.env.PORT || 2000, function () {
  console.log(
    'Your app is listening on port ' + listener.address().port
  );
});

app.listen(() => console.log("I'm Ready To Work..! 24H"));

app.get('/', (req, res) => {
  res.send(`
  <body>
  <center><h1>Bot 24H ON!</h1></center
  </body>`);
});

client.on('ready', async () => {
  console.clear();
  console.log(`${client.user.tag} - rich presence started!`);

  const r = new Discord.RichPresence()
    .setApplicationId('1265825059692609587')
    .setType('PLAYING')
    .setURL('https://www.twitch.tv/apparentlyjack_rl')
    .setState('Hey Nitro is here')
    .setName('quaaxz')
    .setDetails(`Nitro is now`)
    .setStartTimestamp(Date.now())
    .setAssetsLargeImage(
      'https://media.discordapp.net/attachments/1041035673118965772/1270521845841657907/image_2.webp?ex=66b4012d&is=66b2afad&hm=c0fa475d23f70fc777bcea2e70d9682a9aedf2d565ae549e64552ac303361d2b&=&format=webp&width=696&height=379'
    )
    .setAssetsLargeText('Nitro')
    .setAssetsSmallImage(
      'https://media.discordapp.net/attachments/1041035673118965772/1270522062095781990/checked.png?ex=66b40160&is=66b2afe0&hm=1413e3f740030479e77899e2e3bebeb05f97a80e7c0b828e6ec6e9012f86255d&=&format=webp&quality=lossless&width=768&height=768'
    )
    .setAssetsSmallText('Small Text')
    .addButton('Google', 'https://google.com');

  client.user.setActivity(r);
  client.user.setPresence({ status: "dnd" });
});

const mySecret = process.env['TOKEN'];

client.login(mySecret);
  
