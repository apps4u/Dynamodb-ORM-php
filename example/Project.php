<?php

namespace main\model;

use main\orm\DynamoORM;

class Project extends DynamoORM
{
    public $pid      = "";
    public $title    = "";
    public $category = "";
    public $start    = "";
    public $end      = "";
    
    /**
     * constructor
     */
    public function __construct()
    {
    }
    
    /**
     * implementation of the abstract method
     */
    public function tableName()
    {
        return 'project';
    }
    
    /**
     * implementation of the abstract method
     */
    public function rules()
    {
        return array(
            'required' => array(
                self::HASHKEY   => 'cid',
                self::RANGEKEY  => 'pid'
            ),
            'type' => array(
                'pid'       => \AmazonDynamoDB::TYPE_STRING,
                'title'     => \AmazonDynamoDB::TYPE_STRING,
                'category'  => \AmazonDynamoDB::TYPE_STRING,
                'start'     => \AmazonDynamoDB::TYPE_STRING,
                'end'       => \AmazonDynamoDB::TYPE_STRING,
            )
        );
    }
    
    /**
     * implementation of the abstract method
     */
    public function validateAttributes()
    {
        return true;
    }

}