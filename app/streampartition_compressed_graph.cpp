/******************************************************************************
 * streampartition_compressed_graph.cpp
 * *
 * Source of KaHIP -- Karlsruhe High Quality Partitioning.
 * Marcelo Fonseca Faraj <marcelofaraj@gmail.com>
 *****************************************************************************/

#include <argtable3.h>
#include <fstream>
#include <iostream>
#include <math.h>
#include <regex.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <vector>

#include "balance_configuration.h"
#include "data_structure/graph_access.h"
#include "graph_io_stream.h"
#include "parse_parameters.h"
#include "partition/graph_partitioner.h"
#include "partition/partition_config.h"
#include "quality_metrics.h"
#include "random_functions.h"
#include "timer.h"
#include "tools/flat_buffer_writer.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <apps/io/shm_compressed_graph_binary.h>

#define MIN(A, B) ((A) < (B)) ? (A) : (B)
#define MAX(A, B) ((A) > (B)) ? (A) : (B)

void config_multibfs_initial_partitioning(PartitionConfig &partition_config);

long getMaxRSS();

std::string extractBaseFilename(const std::string &fullPath);

namespace {
void setup(PartitionConfig &partition_config, EdgeWeight &total_edge_cut,
           const auto &compressed_graph) {
  partition_config.remaining_stream_nodes = compressed_graph.n();
  partition_config.remaining_stream_edges = compressed_graph.m() / 2;
  partition_config.remaining_stream_ew = 0; // Assume unweighted graph

  partition_config.total_nodes = partition_config.remaining_stream_nodes;
  partition_config.total_edges = partition_config.remaining_stream_edges;

  if (!partition_config.filename_output.compare("")) {
    partition_config.filename_output = "tmp_output.txt";
  }

  if (partition_config.stream_nodes_assign == NULL) {
    partition_config.stream_nodes_assign = new std::vector<PartitionID>(
        partition_config.remaining_stream_nodes, INVALID_PARTITION);
  }
  if (partition_config.stream_blocks_weight == NULL) {
    partition_config.stream_blocks_weight =
        new std::vector<NodeWeight>(partition_config.k, 0);
  }
  if (partition_config.add_blocks_weight == NULL) {
    partition_config.add_blocks_weight =
        new std::vector<NodeWeight>(partition_config.k, 0);
  }
  partition_config.total_stream_nodeweight = 0;
  partition_config.total_stream_nodecounter = 0;
  partition_config.stream_n_nodes = partition_config.remaining_stream_nodes;
  partition_config.total_stream_edges = partition_config.remaining_stream_nodes;

  auto total_weight = (partition_config.balance_edges)
                          ? (partition_config.remaining_stream_nodes +
                             2 * partition_config.remaining_stream_edges)
                          : partition_config.remaining_stream_nodes;

  if (partition_config.num_streams_passes >
      1 + partition_config.restream_number) {
    partition_config.stream_total_upperbound =
        ceil(((100 + 1.5 * partition_config.imbalance) / 100.) *
             (total_weight / (double)partition_config.k));
  } else {
    partition_config.stream_total_upperbound =
        ceil(((100 + partition_config.imbalance) / 100.) *
             (total_weight / (double)partition_config.k));
  }

  partition_config.fennel_alpha =
      partition_config.remaining_stream_edges *
      std::pow(partition_config.k, partition_config.fennel_gamma - 1) /
      (std::pow(partition_config.remaining_stream_nodes,
                partition_config.fennel_gamma));

  partition_config.fennel_alpha_gamma =
      partition_config.fennel_alpha * partition_config.fennel_gamma;

  partition_config.quotient_nodes = partition_config.k;

  total_edge_cut = 0;
  if (partition_config.stream_buffer_len ==
      0) { // signal of partial restream standard buffer size
    partition_config.stream_buffer_len = (LongNodeID)ceil(
        partition_config.remaining_stream_nodes / (double)partition_config.k);
  }
  partition_config.nmbNodes = MIN(partition_config.stream_buffer_len,
                                  partition_config.remaining_stream_nodes);
  partition_config.n_batches = ceil(partition_config.remaining_stream_nodes /
                                    (double)partition_config.nmbNodes);
  partition_config.curr_batch = 0;
  //	partition_config.stream_global_epsilon =
  //(partition_config.imbalance)/100.;
}

std::vector<std::vector<LongNodeID>> *
load_neighborhoods(PartitionConfig &partition_config,
                   const LongNodeID num_lines, const auto &compressed_graph) {
  auto *input = new std::vector<std::vector<LongNodeID>>(num_lines);

  const auto u = compressed_graph.n() - partition_config.remaining_stream_nodes;
  for (LongNodeID i = 0; i < num_lines; ++i) {
    auto &neighbors = (*input)[i];
    neighbors.clear();
    compressed_graph.adjacent_nodes(u + i, [&](const auto adjacent_node) {
      neighbors.push_back(adjacent_node + 1);
    });
  }

  return input;
}

EdgeWeight edge_cut(PartitionConfig &config, const auto &compressed_graph) {
  static_assert(sizeof(kaminpar::shm::EdgeWeight) <= sizeof(EdgeWeight));
  EdgeWeight cut = 0;

  for (const auto u : compressed_graph.nodes()) {
    const PartitionID partitionIDSource = (*config.stream_nodes_assign)[u];

    compressed_graph.adjacent_nodes(u, [&](const auto v, const auto w) {
      const PartitionID partitionIDTarget = (*config.stream_nodes_assign)[v];
      cut += (partitionIDSource != partitionIDTarget) ? w : 0;
    });
  }

  return cut / 2;
}

void run_heistream(const std::size_t child, std::string &graph_filename,
                   PartitionConfig &partition_config,
                   const auto &compressed_graph) {
  partition_config.seed = child;
  srand(partition_config.seed);
  random_functions::setSeed(partition_config.seed);

  std::stringstream ss_log_name;
  ss_log_name << "log-seed" << child << ".log";
  std::ofstream ofs(ss_log_name.str());
  std::cout.rdbuf(ofs.rdbuf());

  std::stringstream ss_partition_name;
  ss_partition_name << "partition-seed" << child << ".log";
  partition_config.filename_output = ss_partition_name.str();

  timer t, processing_t, io_t, model_t;
  double global_mapping_time = 0;
  double buffer_io_time = 0;
  double model_construction_time = 0;

  EdgeWeight total_edge_cut = 0;
  quality_metrics qm;
  balance_configuration bc;

  std::vector<std::vector<LongNodeID>> *input = NULL;
  graph_access *G = new graph_access();

  int &passes = partition_config.num_streams_passes;
  for (partition_config.restream_number = 0;
       partition_config.restream_number < passes;
       partition_config.restream_number++) {

    // IO operations
    io_t.restart();
    setup(partition_config, total_edge_cut, compressed_graph);
    buffer_io_time += io_t.elapsed();

    while (partition_config.remaining_stream_nodes) {
      // IO operations
      io_t.restart();
      partition_config.nmbNodes = MIN(partition_config.stream_buffer_len,
                                      partition_config.remaining_stream_nodes);
      input = load_neighborhoods(partition_config, partition_config.nmbNodes,
                                 compressed_graph);
      buffer_io_time += io_t.elapsed();

      t.restart();

      // build model
      G->set_partition_count(partition_config.k);
      model_t.restart();
      graph_io_stream::createModel(partition_config, *G, input);
      model_construction_time += model_t.elapsed();
      graph_io_stream::countAssignedNodes(partition_config);
      graph_io_stream::prescribeBufferInbalance(partition_config);
      bool already_fully_partitioned = (partition_config.restream_vcycle &&
                                        partition_config.restream_number);
      bc.configurate_balance(partition_config, *G,
                             already_fully_partitioned ||
                                 !partition_config.stream_initial_bisections);
      config_multibfs_initial_partitioning(partition_config);

      // perform partitioning
      graph_partitioner partitioner;
      partitioner.perform_partitioning(partition_config, *G);

      // permanent assignment
      graph_io_stream::generalizeStreamPartition(partition_config, *G);

      global_mapping_time += t.elapsed();

      // write batch partition to the disc
      if (partition_config.stream_output_progress) {
        std::stringstream filename;
        if (!partition_config.filename_output.compare("")) {
          filename << "tmppartition" << partition_config.k << "-"
                   << partition_config.curr_batch;
        } else {
          filename << partition_config.filename_output << "-"
                   << partition_config.curr_batch;
        }
        if (partition_config.restream_number) {
          filename << ".R" << partition_config.restream_number;
        }
        graph_io_stream::writePartitionStream(partition_config);
      }
    }

    if (partition_config.ram_stream) {
      delete input;
    }
  }

  double total_time = processing_t.elapsed();
  delete G;
  long maxRSS = getMaxRSS();
  FlatBufferWriter fb_writer;

  total_edge_cut = edge_cut(partition_config, compressed_graph);
  fb_writer.updateVertexPartitionResults(
      total_edge_cut,
      qm.balance_full_stream(*partition_config.stream_blocks_weight));

  // write the partition to the disc
  std::stringstream filename;
  if (!partition_config.filename_output.compare("")) {
    filename << "tmppartition" << partition_config.k;
  } else {
    filename << partition_config.filename_output;
  }

  if (!partition_config.suppress_output) {
    graph_io_stream::writePartitionStream(partition_config);
  } else {
    std::cout << "No partition will be written as output." << std::endl;
  }

  if (partition_config.ghostkey_to_edges != NULL) {
    delete partition_config.ghostkey_to_edges;
  }

  fb_writer.updateResourceConsumption(buffer_io_time, model_construction_time,
                                      global_mapping_time, global_mapping_time,
                                      total_time, maxRSS);
  fb_writer.write(graph_filename, partition_config);
}
} // namespace

int main(int argn, char **argv) {
  std::cout << R"(
██   ██ ███████ ██ ███████ ████████ ██████  ███████  █████  ███    ███
██   ██ ██      ██ ██         ██    ██   ██ ██      ██   ██ ████  ████
███████ █████   ██ ███████    ██    ██████  █████   ███████ ██ ████ ██
██   ██ ██      ██      ██    ██    ██   ██ ██      ██   ██ ██  ██  ██
██   ██ ███████ ██ ███████    ██    ██   ██ ███████ ██   ██ ██      ██


███    ██  ██████  ██████  ███████
████   ██ ██    ██ ██   ██ ██
██ ██  ██ ██    ██ ██   ██ █████
██  ██ ██ ██    ██ ██   ██ ██
██   ████  ██████  ██████  ███████
    )" << std::endl;

  PartitionConfig partition_config;
  std::string graph_filename;

  bool is_graph_weighted = false;
  bool suppress_output = false;
  bool recursive = false;
  int ret_code =
      parse_parameters(argn, argv, partition_config, graph_filename,
                       is_graph_weighted, suppress_output, recursive);
  if (ret_code) {
    return 0;
  }

  partition_config.LogDump(stdout);
  partition_config.graph_filename = graph_filename;
  partition_config.stream_input = true;

  const auto &compressed_graph =
      kaminpar::shm::io::compressed_binary::read(graph_filename);

  const std::size_t num_processes = 5;
  pid_t children[num_processes];

  for (std::size_t i = 0; i < num_processes; ++i) {
    pid_t child = fork();

    if (child == 0) {
      std::streambuf *coutbuf = std::cout.rdbuf();
      std::cout << "Starting seed " << i << std::endl;

      run_heistream(i, graph_filename, partition_config, compressed_graph);

      std::cout.rdbuf(coutbuf);
      std::cout << "Seed " << i << " finished" << std::endl;

      _exit(EXIT_SUCCESS);
    } else if (child == -1) {
      std::cerr << "Failed to fork" << std::endl;
      std::exit(EXIT_FAILURE);
    }

    children[i] = child;
  }

  for (std::size_t i = 0; i < num_processes; ++i) {
    pid_t child = children[i];

    int status;
    waitpid(child, &status, 0);

    if (status != EXIT_SUCCESS) {
      std::cerr << "Seed " << i << " exited with failure code" << std::endl;
      std::exit(EXIT_FAILURE);
    }
  }

  return EXIT_SUCCESS;
}

void config_multibfs_initial_partitioning(PartitionConfig &partition_config) {
  if (partition_config.initial_part_multi_bfs &&
      partition_config.curr_batch >= 2) {
    partition_config.initial_partitioning_type = INITIAL_PARTITIONING_MULTIBFS;
  }
}

long getMaxRSS() {
  struct rusage usage;

  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    // The maximum resident set size is in kilobytes
    return usage.ru_maxrss;
  } else {
    std::cerr << "Error getting resource usage information." << std::endl;
    // Return a sentinel value or handle the error in an appropriate way
    return -1;
  }
}

// Function to extract the base filename without path and extension
std::string extractBaseFilename(const std::string &fullPath) {
  size_t lastSlash = fullPath.find_last_of('/');
  size_t lastDot = fullPath.find_last_of('.');

  if (lastSlash != std::string::npos) {
    // Found a slash, extract the substring after the last slash
    return fullPath.substr(lastSlash + 1, lastDot - lastSlash - 1);
  } else {
    // No slash found, just extract the substring before the last dot
    return fullPath.substr(0, lastDot);
  }
}
