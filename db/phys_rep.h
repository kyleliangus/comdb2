// extern variable defn
int gbl_is_physical_replicant;

int set_host_db(char* host_db);
int add_replicant_host(char *hostname);
int remove_replicant_host(char *hostname);
void cleanup_hosts();
const char* start_replication();

