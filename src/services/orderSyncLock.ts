const fetchUsers = new Set<string>();
const updateUsers = new Set<string>();

export function assertIdleForFetch(userId: string) {
  if (updateUsers.has(userId)) throw new Error("Order update is already running. Wait or force kill first.");
  if (fetchUsers.has(userId)) throw new Error("Order fetch is already running");
}

export function assertIdleForUpdate(userId: string) {
  if (fetchUsers.has(userId)) throw new Error("Order fetch is already running. Wait or force kill first.");
  if (updateUsers.has(userId)) throw new Error("Order update is already running");
}

export function markFetchRunning(userId: string, running: boolean) {
  if (running) fetchUsers.add(userId);
  else fetchUsers.delete(userId);
}

export function markUpdateRunning(userId: string, running: boolean) {
  if (running) updateUsers.add(userId);
  else updateUsers.delete(userId);
}

export function isSyncRunning(userId: string) {
  return fetchUsers.has(userId) || updateUsers.has(userId);
}
