package com.example.canalexample.dao;

import com.example.canalexample.po.UserPO;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface UserRespository extends ElasticsearchRepository<UserPO,Integer> {
}
