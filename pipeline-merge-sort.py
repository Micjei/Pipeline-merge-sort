import math

class MPI_Processor_Sim:
    def __init__(self, rank, n):
        self.rank = rank
        self.max_q_len = 1 << (rank - 1)
        self.queues = [[], []]
        self.processed_total = 0
        self.q1_proc = 0
        self.q2_proc = 0
        self.current_q_id = 0
        self.state = "WAIT_Q1"

    def receive(self, val, tag):
        self.queues[tag].append(val)

    def work(self):
        Q1_id = self.current_q_id % 2
        Q2_id = (self.current_q_id + 1) % 2
        Q1 = self.queues[Q1_id]
        Q2 = self.queues[Q2_id]
        res = None

        if self.state == "WAIT_Q1":
            if len(Q1) >= self.max_q_len:
                self.state = "WAIT_Q2_FIRST"
        
        if self.state == "WAIT_Q2_FIRST":
            if len(Q2) >= 1:
                self.state = "MERGING"

        if self.state == "MERGING":
            if self.q1_proc < self.max_q_len and self.q2_proc < self.max_q_len:
                # OPRAVA: Posíláme VĚTŠÍ prvek dál
                if Q1[0] > Q2[0]:
                    res = Q1.pop(0)
                    self.q1_proc += 1
                else:
                    res = Q2.pop(0)
                    self.q2_proc += 1
            else:
                self.state = "FLUSH_REST"

        if self.state == "FLUSH_REST":
            if self.q1_proc < self.max_q_len:
                if Q1:
                    res = Q1.pop(0)
                    self.q1_proc += 1
            elif self.q2_proc < self.max_q_len:
                if Q2:
                    res = Q2.pop(0)
                    self.q2_proc += 1
            
            if self.q1_proc == self.max_q_len and self.q2_proc == self.max_q_len:
                self.q1_proc = 0
                self.q2_proc = 0
                self.current_q_id += 1
                self.processed_total += 2 * self.max_q_len
                self.state = "WAIT_Q1"
        return res

def run_pms(data):
    n = len(data)
    num_procs = int(math.log2(n))
    procs = [MPI_Processor_Sim(i + 1, n) for i in range(num_procs)]
    input_data = list(data)
    final_output = []
    
    print(f"{'Krok':<5} | {'P1 [Q1|Q2]':^12} | {'P2 [Q1|Q2]':^12} | {'P3 [Q1|Q2]':^12} | Výstup")
    print("-" * 85)

    for step in range(1, 40):
        # 1. Výpočet (co procesory vyprodukují v tomto taktu)
        moving = [p.work() for p in procs]
        
        # 2. Vstup do P1 (střídá QUEUE1 a QUEUE2 po 1 prvku)
        if input_data:
            val = input_data.pop(0)
            tag = (n - len(input_data) - 1) % 2
            procs[0].receive(val, tag)

        # 3. Posun mezi P_i -> P_{i+1}
        for i in range(num_procs):
            val = moving[i]
            if val is not None:
                if i == num_procs - 1:
                    final_output.append(val)
                else:
                    p_next = procs[i+1]
                    # Tagy pro další procesor se střídají po jeho max_q_len
                    # Počet prvků, které už p_next celkem přijal (včetně těch v queues a už zpracovaných)
                    received_so_far = sum(len(q) for q in p_next.queues) + \
                                      p_next.q1_proc + p_next.q2_proc + \
                                      (p_next.current_q_id * 2 * p_next.max_q_len)
                    tag = (received_so_far // p_next.max_q_len) % 2
                    p_next.receive(val, tag)

        # Výpis stavu front
        states = []
        for p in procs:
            q1 = "".join(map(str, p.queues[0]))
            q2 = "".join(map(str, p.queues[1]))
            states.append(f"[{q1:>3}|{q2:<3}]")
        
        print(f"{step:<5} | {'   |   '.join(states)} | {final_output}")
        if len(final_output) == n: break

# Data z tvého ručního postupu (druhý obrázek)
run_pms([6, 4, 7, 8, 2, 3, 5, 1])