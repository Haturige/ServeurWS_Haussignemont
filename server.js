// Serveur multi-joueur pour la visite virtuelle.
// Ne gère AUCUN fichier 3D — uniquement la position/nom/couleur/pièce de
// chaque joueur connecté, qu'il retransmet à tous les autres.

const os = require("os");

const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  console.log(
    `[HTTP] ${new Date().toLocaleTimeString()} - ${req.method} ${req.url}`
  );

  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8",
  });

  res.end("Serveur multi-joueur de la visite virtuelle — en ligne.");
});

const wss = new WebSocketServer({ server });

const players = new Map();
// id -> { ws, name, color, x, y, z, ry, room, clientId }

let nextId = 1;

const log = (...args) => {
  console.log(
    `[${new Date().toLocaleTimeString()}]`,
    ...args
  );
};

const broadcastToAll = (payload) => {
  const raw = JSON.stringify(payload);
  players.forEach((p) => {
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(raw);
  });
};

wss.on("connection", (ws, request) => {
  const id = nextId++;

  const ip =
    request.headers["x-forwarded-for"] ||
    request.socket.remoteAddress ||
    "inconnue";

  players.set(id, {
    ws,
    name: "Invité",
    color: "#ffffff",
    x: 0,
    y: 0,
    z: 0,
    ry: 0,
    room: null,
    eyeHeight: 1.7,
    clientId: null,
  });

  log(
    `🟢 Joueur connecté | ID=${id} | IP=${ip} | Joueurs=${players.size}`
  );

  ws.send(
    JSON.stringify({
      type: "welcome",
      id,
    })
  );

  log(`➡️ Message WELCOME envoyé au joueur ${id}`);

  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw);
    } catch (error) {
      log(
        `⚠️ Message JSON invalide reçu du joueur ${id}:`,
        raw.toString()
      );
      return;
    }

    const player = players.get(id);

    if (!player) {
      log(`⚠️ Message reçu pour un joueur inexistant : ${id}`);
      return;
    }

    // -------------------------
    // JOIN
    // -------------------------

    if (msg.type === "join") {
      // Identifiant persistant côté navigateur (un par onglet, voir le
      // client). S'il correspond à une AUTRE connexion déjà active, c'est
      // très probablement le même onglet qui se reconnecte après une
      // connexion "zombie" (AFK, mise en veille, coupure réseau...) — on
      // ferme l'ancienne immédiatement plutôt que d'attendre que le
      // ping/pong la détecte comme morte (ce qui pouvait prendre jusqu'à
      // 15-30 secondes et laissait un doublon visible entre-temps).
      const incomingClientId =
        typeof msg.clientId === "string" && msg.clientId
          ? msg.clientId.slice(0, 64)
          : null;

      if (incomingClientId) {
        players.forEach((otherPlayer, otherId) => {
          if (otherId === id) return;
          if (otherPlayer.clientId !== incomingClientId) return;

          log(
            `🧹 Doublon détecté (même clientId) : fermeture immédiate de l'ancienne connexion ID=${otherId} au profit de ID=${id}`
          );

          try {
            otherPlayer.ws.terminate();
          } catch (error) {
            log(`⚠️ Erreur en fermant l'ancienne connexion ID=${otherId} :`, error.message);
          }

          // Supprimé tout de suite (au lieu d'attendre l'event "close" async
          // de cette socket) pour qu'aucun broadcast intermédiaire ne le
          // renvoie encore aux autres clients.
          players.delete(otherId);
        });
      }

      player.clientId = incomingClientId;

      const oldName = player.name;

      player.name = String(msg.name || "Invité").slice(0, 24);

      player.color =
        /^#[0-9a-fA-F]{6}$/.test(msg.color)
          ? msg.color
          : "#ffffff";

      log(
        `👋 JOIN | ID=${id} | Nom="${player.name}" | Couleur=${player.color}`
      );

      if (oldName !== player.name) {
        log(
          `✏️ Joueur ${id} identifié comme "${player.name}"`
        );
      }
      broadcastToAll({
        type: "chat",
        kind: "system",
        id,
        text: `${player.name} a rejoint la visite`,
        ts: Date.now(),
      });

    // -------------------------
    // UPDATE
    // -------------------------

    } else if (msg.type === "update") {

      const oldRoom = player.room;

      player.x = Number(msg.x) || 0;
      player.y = Number(msg.y) || 0;
      player.z = Number(msg.z) || 0;
      player.ry = Number(msg.ry) || 0;
      player.eyeHeight = Number(msg.eyeHeight) || 1.7;
      if (/^#[0-9a-fA-F]{6}$/.test(msg.color)) {
        player.color = msg.color;
      }

      player.room = msg.room
        ? String(msg.room).slice(0, 64)
        : null;

      // On log seulement les changements de pièce.
      // Ne pas logger chaque position : 10 fois/seconde/joueur = énorme spam.
      if (oldRoom !== player.room) {
        log(
          `🚪 CHANGEMENT DE PIÈCE | ${player.name} (ID=${id}) | ` +
          `"${oldRoom || "aucune"}" → "${player.room || "aucune"}"`
        );
      }

    } else if (msg.type === "chat") {
      const text = String(msg.text || "").trim().slice(0, 300);
      if (!text) return;

      log(`💬 CHAT | ${player.name} (ID=${id}) : ${text}`);

      broadcastToAll({
        type: "chat",
        kind: "message",
        id,
        name: player.name,
        color: player.color,
        text,
        ts: Date.now(),
      });

    } else {
      log(
        `❓ Type de message inconnu reçu du joueur ${id}:`,
        msg.type
      );
    }
  });

  ws.on("error", (error) => {
    log(
      `🔴 Erreur WebSocket | Joueur ${id} (${players.get(id)?.name || "Inconnu"})`,
      error.message
    );
  });

  ws.on("close", (code, reason) => {
    const player = players.get(id);

    const playerName = player?.name || "Inconnu";

    log(
      `🔴 Joueur déconnecté | ID=${id} | Nom="${playerName}" | ` +
      `Code=${code} | Raison="${reason.toString() || "aucune"}"`
    );

    broadcastToAll({
      type: "chat",
      kind: "system",
      id,
      text: `${playerName} a quitté la visite`,
      ts: Date.now(),
    });

    players.delete(id);

    log(`👥 Joueurs restants : ${players.size}`);
  });
});


// ==================================================
// DIFFUSION DE L'ÉTAT DES JOUEURS
// ==================================================

let broadcastCount = 0;

setInterval(() => {
  if (players.size === 0) return;

  const states = [];

  players.forEach((p, pid) => {
    states.push({
      id: pid,
      name: p.name,
      color: p.color,
      x: p.x,
      y: p.y,
      z: p.z,
      ry: p.ry,
      room: p.room,
      eyeHeight: p.eyeHeight,
    });
  });

  const raw = JSON.stringify({
    type: "state",
    players: states,
  });

  let sentCount = 0;

  players.forEach((p, pid) => {
    if (p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(raw);
      sentCount++;
    } else {
      log(
        `⚠️ Impossible d'envoyer l'état au joueur ${pid} : WebSocket non ouvert`
      );
    }
  });

  broadcastCount++;

  // Log seulement toutes les 5 secondes environ
  // 50 broadcasts × 100 ms = 5 secondes
  if (broadcastCount >= 50) {
    log(
      `📡 État diffusé | Joueurs=${players.size} | Destinataires=${sentCount}`
    );

    players.forEach((p, id) => {
      log(
        `   └─ ID=${id} | ${p.name} | ` +
        `Pièce=${p.room || "?"} | ` +
        `Position=(${p.x.toFixed(2)}, ${p.y.toFixed(2)}, ${p.z.toFixed(2)})`
      );
    });

    broadcastCount = 0;
  }

}, 100); // 10 fois par seconde


setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 15000);


// ==================================================
// GESTION DES ERREURS DU SERVEUR
// ==================================================

server.on("error", (error) => {
  console.error("");
  console.error("❌ ERREUR SERVEUR");
  console.error(error);
  console.error("");
});


// ==================================================
// DÉMARRAGE
// ==================================================

function getLocalIPs() {
  const interfaces = os.networkInterfaces();
  const addresses = [];

  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      // On garde uniquement les adresses IPv4 non internes
      if (iface.family === "IPv4" && !iface.internal) {
        addresses.push({
          interface: name,
          address: iface.address,
        });
      }
    }
  }

  return addresses;
}

server.listen(PORT, () => {
  console.log("");
  console.log("==============================================");
  console.log(" 🏠 SERVEUR MULTI-JOUEUR - VISITE VIRTUELLE");
  console.log("==============================================");

  console.log("");
  console.log("📍 Adresse locale :");
  console.log(`   http://localhost:${PORT}`);
  console.log(`   ws://localhost:${PORT}`);

  const localIPs = getLocalIPs();

  if (localIPs.length > 0) {
    console.log("");
    console.log("🌐 Adresses accessibles sur le réseau local :");

    localIPs.forEach(({ interface, address }) => {
      console.log(`   ${interface}`);
      console.log(`   HTTP : http://${address}:${PORT}`);
      console.log(`   WS   : ws://${address}:${PORT}`);
    });
  } else {
    console.log("");
    console.log("⚠️ Aucune adresse IPv4 locale détectée.");
  }

  console.log("");
  console.log("==============================================");
  console.log("");
});

process.on("SIGTERM", () => {
  log("🛑 Arrêt du serveur (SIGTERM)");
  wss.clients.forEach((ws) => ws.close(1001, "Redémarrage du serveur"));
  server.close(() => process.exit(0));
});