#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

RaftServer::RaftServer(Frame * frame) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

  // Initialize member variables
  // Examine should we move this one to setup function
  mtx_.lock();
  m_currentRole = Role::FOLLOWER;
  m_currentTerm = 0;
  m_voteFor = -1; // -1 mean NULL, vote for no one
  if (m_log.size() != 0) m_log.clear();

  m_commitIndex = 0; // 1-indexed
  m_lastApplied = 0; // 1-indexed
  
  if (m_receivedVote.size() != 0) m_receivedVote.clear();
  mtx_.unlock();
}

//===================================================================
/* Default server code*/

RaftServer::~RaftServer() {
  /* Your code here for server teardown */
  /* When server teardown -> it should save its persistent state to disk?
  What if it suddently crashes? 
  */

}


void RaftServer::Setup() {
  /* Your code here for server setup. Due to the asynchronous nature of the 
     framework, this function could be called after a RPC handler is triggered. 
     Your code should be aware of that. This function is always called in the 
     same OS thread as the RPC handlers. */
  // if (m_currentTime.time_since_epoch() == decltype(m_currentTime)::duration::zero()){
  //   m_currentTime = std::chrono::high_resolution_clock::now();
  // }
  Coroutine::Sleep(2000000); // Idle period to make sure all server finished set up
  BecomeFollower(0, Role::LEADER); // Set to leader to run Background Ticker
}


bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  // Code to get EntryLog from client
  if (m_currentRole == Role::LEADER){
    // Get cmd from Marshal object
    int cmdData = GetCmd(cmd);
    // Add to log
    // mtx_.lock();
    m_log.push_back(LogEntry(m_currentTerm, cmdData)); 
    *index = m_log.size(); // 1-indexed
    *term = m_currentTerm;
    // mtx_.unlock();
    #pragma region DEBUG
    if (m_displayLog) Log_info("[WARN]  %s %d added cmd (index: %d, term: %d, cmd: %d), m_log size %d", PrintCurrentRole().c_str(), loc_id_, *index, *term, cmdData, m_log.size());
    #pragma endregion
    
    return true;
  } 
  return false;
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  *is_leader = m_currentRole == Role::LEADER;
  *term = m_currentTerm;
}

//===================================================================
/* Changing State (Role) functions */
void RaftServer::BecomeFollower(int64_t newTerm, Role fromRole){
  mtx_.lock();
  m_currentRole = Role::FOLLOWER;
  ResetInternalState(newTerm);
  ResetTickerDuration();
  ResetTimestamp();
  mtx_.unlock();
  if (fromRole == Role::LEADER){
    StartBackgroundTicker(); // Start background ticker when step back from a leader
    if (m_displayLog) Log_info("[WARN]  leader %d steps down to follower, timer %d", loc_id_, m_isTimerRunning);
  }
}

void RaftServer::StartElection(){
  // This function should run inside a Coroutine
  // Clear internal state first (in case running from previous candidate session)
  mtx_.lock();
  m_currentRole = Role::CANDIDATE;
  ResetTickerDuration();
  ResetTimestamp();

  m_currentTerm ++;
  m_receivedVote.clear();
  int64_t currentTerm = m_currentTerm; // Saved the current term in case term changed (i.e receiving higher term AE)
  uint64_t lastLogIndex = m_log.size() - 1; 
  uint64_t lastLogTerm = 0;
  if (!m_log.empty()) lastLogTerm = m_log.back().term;

  // * Vote for itself
  m_voteFor = loc_id_;
  m_receivedVote.insert(loc_id_); 
  mtx_.unlock();

  #pragma region DEBUG
  if (m_displayLog) Log_info("[WARN]  %s %d dis %d timer %d starts election process at term %d, timeout %d", PrintCurrentRole().c_str(), loc_id_, disconnected_, m_isTimerRunning, m_currentTerm, m_tickerDuration);
  #pragma endregion

  for (uint64_t serverId : m_peerIds){
    if (serverId == loc_id_){
      continue;
    }
    CallRequestVote(serverId, currentTerm, lastLogIndex, lastLogTerm);
  }
}

void RaftServer::StartLeader(){
  // Reset internal state when was a Candidate
  // Extra safety because it'll be cleared when going down to a follower
  if (m_currentRole == Role::CANDIDATE){
    mtx_.lock();
    m_currentRole = Role::LEADER;
    ResetInternalState(-1); // reset without changing term

    // Init leader volatile state
    m_nextIndex.fill(m_commitIndex); // commitIndex is 1-index
    m_matchIndex.fill(-1);
    mtx_.unlock();

    StartLeaderTicker();
  }
}

//===================================================================
/* Reset variables functions */
void RaftServer::ResetInternalState(int64_t newTerm){
  // Need to call inside a mutex lock
  if (newTerm >= 0) m_currentTerm = newTerm; // Only valid newTerm
  m_voteFor = -1;
  m_receivedVote.clear();
}

void RaftServer::ResetTimestamp(){
  m_currentTime = std::chrono::high_resolution_clock::now();
}

void RaftServer::ResetTickerDuration(){
  m_tickerDuration = rand() % m_electionTimerRange + m_electionTimerOffset;
  if (m_currentRole == Role::CANDIDATE){
    m_tickerDuration += 3 * m_resendRVInterval/1000; // Add time for 3 rounds more of resend RV for candidate
  }
  #pragma region DEBUG
  if (m_displayLog) Log_info("[INFO]  %s %d resets its ticker to %d ms", PrintCurrentRole().c_str(), loc_id_, m_tickerDuration);
  #pragma endregion
}

void RaftServer::BroadcastAppendEntry(){
  /* Server sending AE Rpc for both Heart Beat and Append Logs */
  // Saved current term when issue an Rpc
  // In case term changed before sending Rpc to all servers
  if (m_currentRole != Role::LEADER) return;
  mtx_.lock();
  int64_t currentTerm = m_currentTerm;
  mtx_.unlock();
  #pragma region DEBUG
  if (m_displayLog) Log_info("[DEBUG] %s %d dis %d broadcasts AE with log %s", PrintCurrentRole().c_str(), loc_id_, disconnected_, PrintLog().c_str());

  #pragma endregion
  for (auto serverId : m_peerIds){
    if (serverId != loc_id_){
      CallAppendEntry(serverId, currentTerm);
    }
  }
}

void RaftServer::CallAppendEntry(uint64_t serverId, int64_t currentTerm){
  Coroutine::CreateRun([this, serverId, currentTerm](){
    // Retrieve AE Rpc args for each follower
    mtx_.lock();
    int64_t peerNextIndex = m_nextIndex[serverId]; // 0-indexed -> per server
    int64_t prevLogIndex = peerNextIndex - 1; // 0-indexed
    int64_t prevLogTerm = 0; // 1-indexed
    if (prevLogIndex > -1) prevLogTerm = m_log[prevLogIndex].term;
    int64_t leaderCommit = m_commitIndex; // 1-indexed

    // Preparing log to send
    std::vector<LogEntry> sendLog(m_log.begin() + prevLogIndex + 1, m_log.end());
    mtx_.unlock();

    std::vector<int64_t> sendTerm;
    std::vector<int64_t> sendCmd;
    SplitFromLog(sendLog, sendTerm, sendCmd);

    // Var for receiving reply from  follower
    int64_t replyTerm;
    bool_t replySuccess;
    int64_t replyConflictTerm;
    int64_t replyConflictIndex;

    // Three cases:
    // 1. If replyTerm > savedCurrentTerm -> Become a Follower
    // 2. If reply is false and replyTerm == -1 -> Follower disconnected
    //    Try to resend the same Rpc
    // 3. If reply is false and replyTerm == savedCurrentTerm -> decrease prevLogIndex by 1 and update prevLogTerm -> Send AE Rpc
    // 4. If reply is true and replyTerm == savedCurrentTerm -> Success
    // 5. If reply is true and replyTerm != savedCurrentTerm -> Able to happen?
    #pragma region DEBUG
    if (m_displayLog and not disconnected_) Log_info("[INFO]  SendAE: %s %d -> %d, dis? %d (prevLog (Idx:%d, Term:%d), sendLogLength %d, leaderCommitIndex %d | nextIndex %d, matchIndex %d)", PrintCurrentRole().c_str(), loc_id_, serverId, disconnected_, prevLogIndex, prevLogTerm, sendCmd.size(), leaderCommit, m_nextIndex[serverId], m_matchIndex[serverId]);
    #pragma endregion

    auto event = commo()->SendAppendEntries(0, serverId, 
                                      loc_id_, // leaderId
                                      currentTerm, 
                                      prevLogIndex, 
                                      prevLogTerm, 
                                      sendTerm, 
                                      sendCmd, 
                                      leaderCommit, 
                                      &replyTerm, 
                                      &replySuccess,
                                      &replyConflictTerm,
                                      &replyConflictIndex);
    
    // event->Wait(m_heartBeatInterval); // Need to send faster to reduce the chance other servers timed out
    event->Wait(1000000);
    if (event->status_ == Event::TIMEOUT) {
      // #pragma region DEBUG
      // if (m_displayLog and not disconnected_) Log_info("[INFO]  ResendAE: %s %d -> %d dis? %d resend AE", PrintCurrentRole().c_str(), loc_id_, serverId, disconnected_);
      // #pragma endregion
      // becasue the error in event.cc line 127
      // maybe the event is not destroyed? so it stayed alive in TIMEOUT status?
      // if (event) event->status_ = Event::DONE; 
      return; 
    } else {
      #pragma region DEBUG
      if (m_displayLog) Log_info("[INFO]  ReceiveAE: %s %d <- %d response (Term: %d, Success: %d, ConflictTerm %d, ConflictIndex %d)", PrintCurrentRole().c_str(), loc_id_, serverId, replyTerm, replySuccess, replyConflictTerm, replyConflictIndex);
      #pragma endregion

      if (replyTerm == 0){
        // Follower is disconnected -> Resend Rpc with the same content
        return;
      }

      if (replyTerm > currentTerm){
        // Become a follower
        BecomeFollower(replyTerm, Role::LEADER);
      }

      if (replyTerm == currentTerm){
        mtx_.lock();
        if (replySuccess){
          // Received success signal. Either it's a heartbeat response or update log response.
    
          #pragma region DEBUG
          int64_t savedNextIndex = m_nextIndex[serverId];
          #pragma endregion
          // Update follower nextIndex and matchIndex
          int64_t newMatchIndex = prevLogIndex + sendTerm.size();

          m_matchIndex[serverId] = std::max(m_matchIndex[serverId], newMatchIndex); // matchIndex increased monotonically
          m_nextIndex[serverId] = m_matchIndex[serverId] + 1;
          // m_nextIndex[serverId] = prevLogIndex + sendTerm.size() + 1; 
          // m_matchIndex[serverId] = m_nextIndex[serverId] - 1;

          #pragma region DEBUG
          if (m_displayLog and peerNextIndex != m_nextIndex[serverId]) Log_info("[INFO]  %s %d changed nextIndex of server %d: %d -> %d", PrintCurrentRole().c_str(), loc_id_, serverId, savedNextIndex, m_nextIndex[serverId]);
          #pragma endregion

          if (peerNextIndex != m_nextIndex[serverId]){
            // Receiver added new log to its log -> Check if there is any log to commit 
            UpdateCommitIndex(leaderCommit); // Will update m_commitIndex
            Coroutine::Sleep(rand() % 1000); // Ensure that commit cmd block won't kick up in the same time across 4 coroutines.
            if (m_commitIndex > leaderCommit){
              int64_t savedLastApplied = m_lastApplied;
              m_lastApplied = m_commitIndex; // Because of 1-indexed -> Update first to prevent another coroutine start commiting
              for (int i = savedLastApplied; i < m_commitIndex; i++){
                // m_lastApplied and m_commitIndex are 1-indexed
                CommitCmd(m_log[i].cmd);
                #pragma region DEBUG
                if (m_displayLog) Log_info("[WARN]  %s %d commit (index %d, term %d, cmd %d)", PrintCurrentRole().c_str(), loc_id_, i, m_currentTerm, m_log[i].cmd);
                #pragma endregion
              }
            }
          }   
        } else {
          // Log inconsistent -> reduce next index by one
          // m_nextIndex[serverId] = std::max((int64_t)0, m_nextIndex[serverId] - 1);

          // New part: Log inconsistency optimization
          if (replyConflictTerm == 0){
            // Indicating follower log is shorter than leader -> Send up to the newest log
            m_nextIndex[serverId] = replyConflictIndex;
          }

          if (replyConflictTerm > 0){
            // There is a conflict in term 
            // There are two cases:
            // The conflictTerm is in current leader log -> scan for it
            int64_t index;
            for (index = m_log.size() - 1; index >= 0; index--){
              // Scan backward to find the last log with conflicting term
              // Last because the scenario is follower log with conflicting term is longer than leader log
              if (m_log[index].term == replyConflictTerm) break;
            }
            if (index > 0){
              m_nextIndex[serverId] = index + 1;
            } else {
            // or it's not -> overwritten the whole log with conflict term
              m_nextIndex[serverId] = replyConflictIndex;
            }
          }
          m_matchIndex[serverId] = m_nextIndex[serverId] - 1; 
        }
        mtx_.unlock();    
      }
    }
  });
}

void RaftServer::CallRequestVote(uint64_t serverId, int64_t currentTerm, int64_t lastLogIndex, int64_t lastLogTerm){
  Coroutine::CreateRun([this, serverId, currentTerm, lastLogIndex, lastLogTerm](){
    uint64_t replyId;
    int64_t replyTerm;
    bool_t replyGranted;
    bool_t resend = false;

    #pragma region DEBUG
    if (m_displayLog and not disconnected_) Log_info("[INFO]  SendRV: %s %d -> %d, dis %d timer %d (term %d, lastLogIndex %d, lastLogTerm %d)", PrintCurrentRole().c_str(), loc_id_, serverId, disconnected_, m_isTimerRunning, currentTerm, lastLogIndex, lastLogTerm);
    #pragma endregion

    auto event = commo()->SendRequestVote(0, serverId, 
                                          m_currentTerm, 
                                          loc_id_, 
                                          lastLogIndex, 
                                          lastLogTerm, 
                                          &replyId, 
                                          &replyTerm, 
                                          &replyGranted);   
    event->Wait(1000000);
    // event->Wait(m_resendRVInterval);
    if (event->status_ == Event::TIMEOUT) {
      // resend = true;
      // if (event) event->status_ = Event::DONE; // Cause segmentation fault because pointer is not valid?
      return;
    } else {
      // * GET RESPONSE
      #pragma region DEBUG
      if (m_displayLog) {
        int64_t savedDebugTerm = m_currentTerm; // maybe term changed when receive Rpc
        if(m_displayLog) Log_info("[INFO]  ReceiveRV: term %d %s %d <- %d response: (replyTerm: %d, replyGrant: %d)", savedDebugTerm, PrintCurrentRole().c_str(), loc_id_, replyId, replyTerm, replyGranted);
      }
      #pragma endregion

      if (replyTerm == -1){
        // Resend RV Rpc with the same content until either get a valid response or timed out
        resend = true;
        Coroutine::Sleep(m_resendRVInterval);
      }
      else {
        // * GET VALID RESPONSE from a FOLLOWER
        mtx_.lock();
        if (replyTerm > currentTerm && m_currentRole == Role::CANDIDATE){
          // * Received higher term response -> Go back to a follower
          BecomeFollower(replyTerm, Role::CANDIDATE);
          mtx_.unlock();
          return;
        }
        if ((replyTerm == currentTerm) && (m_currentRole == Role::CANDIDATE) && replyGranted){
          // * Received granted vote from a server
          // Still need second check because server state can be changed anytime
          m_receivedVote.insert(replyId);
          if (m_receivedVote.size() >= ceil((m_peerIds.size()+1)/2)){
            // * Become leader
            m_receivedVote.clear(); // Prevent other coroutines to begin leader
            m_currentLeader = loc_id_; // Assign itself as leader
            #pragma region DEBUG
            if (m_displayLog) Log_info("[WARN]  %s %d won the election at term %d", PrintCurrentRole().c_str(),loc_id_, m_currentTerm);
            #pragma endregion
            StartLeader();
            mtx_.unlock();
            return;
          }
        }
        mtx_.unlock();
      }
    }
    if (resend and m_currentRole == Role::CANDIDATE){
      auto now = std::chrono::high_resolution_clock::now();
      auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_currentTime).count();
      if (elapsedTime < m_tickerDuration){
        #pragma region DEBUG
        if (m_displayLog) Log_info("[INFO]  %s %d -> %d run another RV Rpc round", PrintCurrentRole().c_str(), loc_id_, serverId);
        #pragma endregion
        CallRequestVote(serverId, currentTerm, lastLogIndex, lastLogTerm);
        return;
      }
    }
  });
}

//===================================================================
/* Event loop functions */
void RaftServer::StartBackgroundTicker(){
  if (!m_isTimerRunning){
    m_isTimerRunning = true;
    #pragma region DEBUG
    if (m_displayLog) Log_info("[WARN]  %s %d starts timer with duration %d", PrintCurrentRole().c_str(), loc_id_, m_tickerDuration);
    #pragma endregion

    Coroutine::CreateRun([this](){
      uint64_t sleepTime = m_checkTimerInterval;
      while (m_currentRole != Role::LEADER){
        Coroutine::Sleep(sleepTime); 
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_currentTime).count();
        // uint64_t duration = (m_currentRole == Role::FOLLOWER) ? m_followerTickerDuration : m_candidateTickerDuration;
        if ((m_tickerDuration - elapsedTime) * 1000 <= m_checkTimerInterval){
          sleepTime = 1000; // Down to 1ms to precise sleeping
        }
        if (elapsedTime > m_tickerDuration){
          StartElection(); // Either start election from a follower or candidate starts another election round
        }
      }
    });
  }
};

void RaftServer::StartLeaderTicker(){
  m_isTimerRunning = false;
  Coroutine::CreateRun([this](){
    while (m_currentRole == Role::LEADER){
      BroadcastAppendEntry();
      Coroutine::Sleep(m_heartBeatInterval);
    }
  });
}

//===================================================================
/* Helper functions */
void RaftServer::UpdateCommitIndex(int64_t leaderCommit){
  // Scan from leaderCommit to the end of log
  // Run on Leader thread -> Should be another background thread
  for (int64_t N = leaderCommit; N < m_log.size(); N++){
    if (m_log[N].term == m_currentTerm){
      // Check if majority of server in matchIndex >= N
      int matchedCnt = 1;
      for (auto serverId: m_peerIds){
        if (serverId == loc_id_){
          continue;
        }
        if (m_matchIndex[serverId] >= N){
          matchedCnt ++;
        }
      }
      mtx_.lock();
      if (matchedCnt >= ceil((m_peerIds.size()+1)/2) && N > m_commitIndex - 1){
        // A LogEntry that has majority of server committed -> Update commitIndex
        m_commitIndex = N + 1; 
        #pragma region DEBUG
        if (m_displayLog) Log_info("[WARN]  UpdateCommitIndex: %s %d updated commitIndex from %d -> %d", PrintCurrentRole().c_str(), loc_id_, leaderCommit, m_commitIndex);
        #pragma endregion
      }
      mtx_.unlock();
    }
  }
}
  
int RaftServer::GetCmd(shared_ptr<Marshallable>& cmd){
  Marshal mar;
  int cmdData;
  cmd->ToMarshal(mar);
  mar >> cmdData;
  return cmdData;
}

void RaftServer::CommitCmd(int cmdValue){
  // Mimic the function in testconf.cc
  // Idk how it works!
  auto cmdptr = std::make_shared<TpcCommitCommand>();
  auto vpd_p = std::make_shared<VecPieceData>();
  vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
  cmdptr->tx_id_ = cmdValue;
  cmdptr->cmd_ = vpd_p;
  
  auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
  app_next_(*cmdptr_m);
}

void RaftServer::SplitFromLog(std::vector<LogEntry>& source, std::vector<int64_t>& term, std::vector<int64_t>& log){
  term.clear();
  log.clear();
  for (auto cmd : source){
    term.push_back(cmd.term);
    log.push_back(cmd.cmd);
  }
  verify(term.size() == log.size());  
};

std::vector<LogEntry> RaftServer::MergeToLog(std::vector<int64_t> term, std::vector<int64_t> log){
  std::vector<LogEntry> res;
  verify(term.size() == log.size());
  for (int i = 0; i < term.size(); i++){
    res.push_back(LogEntry(term[i], log[i]));
  }
  return res;
}

//===================================================================
/* Debug functions */
std::string RaftServer::PrintCurrentRole(){
  std::string role;
  if (m_currentRole == Role::LEADER){
    role = "leader";
  }
  else if (m_currentRole == Role::CANDIDATE){
    role = "candidate";
  }
  else {
    role = "follower";
  }
  return role;
}

std::string RaftServer::PrintLog(){
  string res = "";
  if (m_printLogEntry){
    for (auto e: m_log){
      res = res + e.ToString() + " ";
    }
    return res;
  } 
  return "N/A";
}

void RaftServer::SyncRpcExample() {
  /* This is an example of synchronous RPC using coroutine; feel free to 
     modify this function to dispatch/receive your own messages. 
     You can refer to the other function examples in commo.h/cc on how 
     to send/recv a Marshallable object over RPC. */
  Coroutine::CreateRun([this](){
    string res;
    int des = 0;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     des, 0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
    } else {
    }
  });
}

//===================================================================
/* Do not modify any code below here */

void RaftServer::Disconnect(const bool disconnect) {
  

  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  RaftCommo *c = (RaftCommo*) commo();
  if (disconnect) {
    #pragma region DEBUG
    if (m_disconnectLog) Log_info("[DEBUG] RaftServer::Disconnect -> %s %d disconnected at term %d timer? %d, m_log size %d", PrintCurrentRole().c_str(), loc_id_, m_currentTerm, m_isTimerRunning, m_log.size());
    #pragma endregion
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    #pragma region DEBUG
    if (m_disconnectLog) Log_info("[DEBUG] RaftServer::Reconnect  -> %s %d reconnected at term %d timer? %d, m_log size %d", PrintCurrentRole().c_str(), loc_id_, m_currentTerm, m_isTimerRunning, m_log.size());
    #pragma endregion
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
}

bool RaftServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
