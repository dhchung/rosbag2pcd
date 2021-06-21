#include <iostream>
#include <ros/ros.h>
#include <pcl/io/io.h>
#include <pcl/io/pcd_io.h>
#include <pcl/point_types.h>
#include <pcl_conversions/pcl_conversions.h>
#include "pcl_ros/publisher.h"
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <string>

#include <boost/filesystem.hpp>

#include <thread>


std::vector<std::thread> threads;
std::vector<bool> running_check;

void save_pcd(sensor_msgs::PointCloud2 cloud_t,
              std::string data_dir, 
              int thread_no) {
    running_check[thread_no] = true;

    pcl::io::savePCDFile(data_dir, cloud_t, Eigen::Vector4f::Zero(),
                         Eigen::Quaternionf::Identity(), true);
    // std::cerr << "Data saved to " << data_dir << std::endl;

    running_check[thread_no] = false;
}

int max_thread = 200;


int main(int argc, char** argv) {
    // if(argc==1) {
    //     return 0;
    // }

    std::string bag_path = "/mnt/sdb/Avikus/20210525/exp1_11_27/exp1_2021-05-25-11-28-06.bag";
    // bag_path = argv[1];
    rosbag::Bag lidar_bag;
    rosbag::View view;
    rosbag::View::iterator view_it;


    try{
        std::cout<<"Opening Bag"<<std::endl;
        lidar_bag.open (bag_path, rosbag::bagmode::Read);
        std::cout<<"Bag Opened"<<std::endl;;
    } 
    catch (const rosbag::BagException&) {
        std::cerr << "Error opening file " << argv[1] << std::endl;
        return (-1);
    }

    // rosbag::View topic_list_view(lidar_bag);
    // std::string target_topic;
    // std::map<std::string, std::string> topic_list;
    // for(rosbag::ConnectionInfo const *ci: topic_list_view.getConnections()) {
    //     topic_list[ci->topic] = ci->datatype;
    // }

    // Topic name : /lidar_port/os_cloud_node/points        sensor_msgs/PointCloud2
    //            : /lidar_starboard/os_cloud_node/points   sensor_msgs/PointCloud2

    std::string target_topic = "/lidar1/os_cloud_node/points";

    view.addQuery(lidar_bag, rosbag::TopicQuery(target_topic));
    view_it = view.begin();

    // std::string output_dir = std::string(argv[3]);
    std::string output_dir = "/mnt/sdb/Avikus/20210525/exp1_11_27/lidar1";
    boost::filesystem::path outpath(output_dir);
    if (!boost::filesystem::exists(outpath))
    {
        if (!boost::filesystem::create_directories(outpath))
        {
            std::cerr << "Error creating directory " << output_dir << std::endl;
            return (-1);
        }
        std::cerr << "Creating directory " << output_dir << std::endl;
    }

    int thread_num = 0;
    int data_no = 0;

    sensor_msgs::PointCloud2 cloud_t;
    while(view_it != view.end()) {

        bool all_thread_running = true;
        int empty_thread = -1;
        for(int i = 0; i < thread_num; ++i) {
            if(running_check[i]){

            } else{
                empty_thread = i;
                all_thread_running = false;
                if(threads[i].joinable()){
                    std::cout<<"wait to join"<<std::endl;
                    threads[i].join();
                    std::cout<<"joined"<<std::endl;
                }
            }
        }


        std::cout<<"New fucking data"<<std::endl;
        sensor_msgs::PointCloud2::ConstPtr cloud = view_it->instantiate<sensor_msgs::PointCloud2>();
        std::cout<<"Read fucking data"<<std::endl;
        if(cloud == NULL) {
            std::cout<<"No fucking cloud"<<std::endl;
            ++view_it;
            continue;
        }
        cloud_t = *cloud;
        std::cerr << "Got " << cloud_t.width * cloud_t.height << " data points in frame " << cloud_t.header.frame_id << " on topic " << view_it->getTopic() << " with the following fields: " << pcl::getFieldsList(cloud_t) << std::endl;

        std::stringstream ss;
        ss << output_dir << "/" << cloud_t.header.stamp << ".pcd";
        std::string data_dir = ss.str();

        int num_running_thread = 0;
        for(int i = 0; i < running_check.size(); ++i) {
            if(running_check[i]) {
                ++num_running_thread;
            }
        }
        std::cout<<"running thread"<<num_running_thread<<std::endl;
        std::cout<<"data_no: "<<data_no<<std::endl;
        ++data_no;

        while(num_running_thread >= max_thread) {
            num_running_thread = 0;
            for(int i = 0; i < running_check.size(); ++i) {
                if(running_check[i]) {
                    ++num_running_thread;
                }
            }
            // std::cout<<"Waiting for thread to end"<<std::endl;
        }

        std::cout<<"No Fucking While Loop"<<std::endl;


        if(all_thread_running) {
            running_check.push_back(false);
            threads.emplace_back(save_pcd, 
                                 cloud_t, 
                                 data_dir,
                                 running_check.size()-1);
            threads[running_check.size()-1].detach();
            ++thread_num;
            std::cout<<"Added Thread"<<std::endl;
        } else {
            threads[empty_thread] = std::thread(save_pcd, 
                                                cloud_t,
                                                data_dir,
                                                empty_thread);
            threads[empty_thread].detach();    
        }


        // pcl::io::savePCDFile(data_dir, cloud_t, Eigen::Vector4f::Zero(),
        //                      Eigen::Quaternionf::Identity(), true);
        ++view_it;
    }


    for(int i = 0; i < threads.size(); ++i) {
        if(threads[i].joinable()){
            threads[i].join();
        }
    }



    std::cout<<"Finished"<<std::endl;
 
    return 0;
}