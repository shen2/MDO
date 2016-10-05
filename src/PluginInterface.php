<?php
namespace MDO;

Interface PluginInterface{
	public function preDbInsert($object);

	public function postDbInsert($object);

	public function preDbUpdate($object);

	public function postDbUpdate($object);

	public function preDbDelete($object);

	public function postDbDelete($object);
}
