#include <thread>
#include <condition_variable>
#include <vector>
#include <queue>
#include <algorithm>
#include <chrono>		// for profiling

using namespace std;
using namespace std::chrono;

#define MAX_MASK_SIZE 100

struct search_engine
{
	// sample: search("f:\\abc.txt","?ad", 8)
	bool search(const string& path, const string& mask, size_t num_threads)
	{
		FILE* f = fopen(path.c_str(), "rb");
		if (!f)
		{
			printf("Cannot open %s\n", path.c_str());
			return false;
		}

		if (mask.size() == 0 || mask.size() > MAX_MASK_SIZE)
		{
			printf("Invalid mask %s\n", mask.c_str());
			return false;
		}

		// Маска не может содержать символа перевода строки
		if (strchr(mask.c_str(), '\n'))
		{
			printf("Mask cannot contain EOL\n");
			return false;
		}

		m_mask = mask;
		printf("Searching started, path %s, mask '%s'\n", path.c_str(), m_mask.c_str());

		milliseconds start_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

		// start threads
		m_threads.reserve(num_threads);
		for (size_t i = 0; i < num_threads; ++i)
		{
			m_threads.emplace_back(&search_engine::thread_func, this, i);
		}

		size_t readbuf_size = 1024 * 1024;		// 1mb
		size_t chunksize = readbuf_size / num_threads;
		size_t bytes_read = 1;
		size_t line = 0;		// current line
		size_t pos = 0;		// current pos in the line

		shared_ptr<char> prevbuf;
		while (bytes_read > 0)
		{
			shared_ptr<char> readbuf((char*)malloc(readbuf_size), free);
			if (readbuf == nullptr)
			{
				printf("No memory\n");
				break;
			}

			if (prevbuf)
			{
				memcpy(readbuf.get(), prevbuf.get() + readbuf_size - overlap(), overlap());
				bytes_read = fread(readbuf.get() + overlap(), 1, readbuf_size - overlap(), f);
				if (bytes_read > 0)
				{
					bytes_read += overlap();
				}
			}
			else
			{
				bytes_read = fread(readbuf.get(), 1, readbuf_size, f);
			}
			prevbuf = readbuf;

			// split buf into chunks and put them into threads
			for (size_t offset = 0; offset < bytes_read && bytes_read - offset >= masksize(); offset += chunksize - overlap())
			{
				add_task(readbuf, line, pos, offset, min(bytes_read - offset, chunksize));
			}
		}

		fclose(f);
		wait_completition();

		milliseconds end_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
		milliseconds t = end_ms - start_ms;
		printf("elapsed time %zdms\n", t.count());
		printf("Searching ended\n");
		return true;
	}

	void print()
	{
		// sort on line & pos
		printf("%zd matches\n", m_result.size());
		sort(m_result.begin(), m_result.end(), [](const thread_result& a, const thread_result& b)->bool
			{
				return ((a.line < b.line) || (a.line == b.line && a.pos < b.pos));
			});

		for (size_t i = 0; i < m_result.size(); i++)
		{
			const thread_result& r = m_result[i];
			printf("%zd %zd %s\n", r.line, r.pos, r.txt);
		}
	}


private:

	inline size_t overlap() const { return m_mask.size() - 1; }
	inline size_t masksize() const { return m_mask.size(); }

	void wait_completition()
	{
		m_quit = true;
		m_taskcv.notify_all();

		for (uint32_t i = 0; i < m_threads.size(); ++i)
		{
			m_threads[i].join();
		}
	}

	// find search string
	bool test_mask(const char* buf)
	{
		for (size_t i = 0; i < m_mask.size(); i++)
		{
			if (buf[i] != m_mask[i] && m_mask[i] != '?')
			{
				return false;
			}
		}
		return true;
	}

	void thread_func(size_t thread_index)
	{
		while (1)
		{
			// wait for task
			unique_lock<mutex> lock(m_task_mutex);
			m_taskcv.wait(lock, [this]()->bool { return !m_tasks.empty() || m_quit; });

			if (m_tasks.empty())
			{
				break;
			}
			else
			{
				auto txt = m_tasks.front();
				m_tasks.pop();
				lock.unlock();

				// searching mask in the chunk
				const char* buf = txt.buf.get() + txt.offset;
				size_t line = txt.line;
				size_t pos = txt.pos;

				for (size_t i = 0; i < txt.chunksize; i++, buf++, pos++)
				{
					if (*buf == '\n')
					{
						line++;
						pos = 0;
					}
					else
					{
						if (txt.chunksize - i >= masksize() && test_mask(buf))
						{
							// lock m_result 
							lock_guard<mutex> lock(m_result_mutex);

							m_result.push_back(thread_result());
							auto& b = m_result.back();
							b.line = line + 1;
							b.pos = pos;
							strncpy(b.txt, buf, m_mask.size());
						}
					}
				}
			}
		}
	}

	void add_task(const shared_ptr<char>& buf, size_t& line, size_t& pos, size_t offset, size_t chunksize)
		// returns lines count in this chunk
	{
		if (chunksize > 0)
		{
			{
				lock_guard<mutex> q_lock(m_task_mutex);
				m_tasks.push({ buf, line, pos, offset, chunksize });
			}
			m_taskcv.notify_one();

			if (chunksize > overlap())
			{
				advance_pos(buf.get() + offset, chunksize - overlap(), line, pos);
			}
		}
	}

	void advance_pos(const char* buf, size_t chunksize, size_t& line, size_t& pos)
	{
		for (size_t i = 0; i < chunksize; i++)
		{
			if (*buf++ == '\n')
			{
				line++;
				pos = 0;
			}
			else
			{
				pos++;
			}
		}
	}

	struct thread_task
	{
		shared_ptr<char> buf;
		size_t line;
		size_t pos;
		size_t offset;
		size_t chunksize;
	};

	// search result
	struct thread_result
	{
		size_t line;
		size_t pos;
		char txt[MAX_MASK_SIZE + 1];
	};

	atomic<bool> m_quit{ false };
	string m_mask;
	vector<thread> m_threads;

	mutex m_result_mutex;
	vector<thread_result> m_result;

	mutex m_task_mutex;
	queue<thread_task> m_tasks;
	condition_variable m_taskcv;
};

int main(int argc, char* argv[])
{
	if (argc < 3)
	{
		printf("Usage: mtfind <path-to-text-file> <mask>\n");
		return -1;
	}

	search_engine se;
	if (se.search(argv[1], argv[2], thread::hardware_concurrency()))
	{
		se.print();
	}
}
