static inline uint64_t timer_read_specific(timer_data_t* obj, unsigned int i) {
	return nk_pmc_read(obj->perf_event_nautk[i]);
}
